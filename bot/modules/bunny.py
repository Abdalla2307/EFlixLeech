#!/usr/bin/env python3
import os
import re
import asyncio
import tempfile
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple, List, Set
from urllib.parse import urlparse

import requests
import psycopg2
from psycopg2 import errors
from pyrogram import filters
from pyrogram.errors import MessageNotModified, UserNotParticipant, ChatAdminRequired
from pyrogram.handlers import MessageHandler
from pyrogram.types import Message, Chat

from bot import bot, CMD_SUFFIX


BUNNY_LIBRARY_ID = int(os.environ.get("BUNNY_LIBRARY_ID", "0") or "0")
BUNNY_API_KEY = os.environ.get("BUNNY_API_KEY", "")
BUNNY_CDN_HOST = os.environ.get("BUNNY_CDN_HOST", "")
POSTGRES_URI = os.environ.get("POSTGRES_URI")
BUNNY_API_BASE = "https://video.bunnycdn.com"
MAX_PARALLEL_IMPORTS = int(os.environ.get("MAX_PARALLEL_IMPORTS", "60"))
ALLOWED_RES = [360, 480, 720, 1080]
WAIT_TIMEOUT_S = int(os.environ.get("BUNNY_WAIT_TIMEOUT_S", "7200"))
WAIT_INTERVAL_S = int(os.environ.get("BUNNY_WAIT_INTERVAL_S", "30"))

_import_semaphore = asyncio.Semaphore(MAX_PARALLEL_IMPORTS)


@dataclass
class VideoQueueItem:
    message: Message
    file_name: str
    file_unique_id: str
    chat_id: int


video_queue: asyncio.Queue = asyncio.Queue()
is_processing = False
processed_videos: Set[str] = set()
auto_process_enabled = True


def _bunny_command(command_name: str) -> List[str]:
    cmds = [command_name]
    if CMD_SUFFIX and command_name != "help":
        cmds.append(f"{command_name}{CMD_SUFFIX}")
    return cmds


def _is_video_message(msg: Message) -> bool:
    if not msg:
        return False
    if msg.video:
        return True
    if msg.document and (msg.document.mime_type or "").startswith("video/"):
        return True
    return False


def _split_name(filename: str) -> Tuple[str, str]:
    if not filename:
        return "video", ".mp4"
    if "." in filename:
        stem, ext = filename.rsplit(".", 1)
        return stem, "." + ext
    return filename, ".mp4"


def _ensure_mp4_ext(ext: str) -> str:
    return ext if ext.lower() == ".mp4" else ".mp4"


def _replace_or_append_res_with_ext(stem: str, res: int, ext: str) -> str:
    ext = _ensure_mp4_ext(ext)
    bracket_pat = r"\[(\d{3,4})p\]"
    if re.search(bracket_pat, stem):
        new_stem = re.sub(bracket_pat, f"[{res}p]", stem)
        return f"{new_stem}{ext}"
    plain_pat = r"(\d{3,4})p"
    if re.search(plain_pat, stem):
        new_stem = re.sub(plain_pat, f"{res}p", stem)
        return f"{new_stem}{ext}"
    return f"{stem}.{res}p{ext}"


async def safe_edit(msg: Message, text: str) -> Message:
    try:
        if getattr(msg, "text", None) == text:
            return msg
        return await msg.edit_text(text)
    except MessageNotModified:
        return msg


def _derive_title_from_url(url: str) -> str:
    try:
        path = urlparse(url).path
        name = os.path.basename(path) or "video"
        name = name.split("?")[0].split("#")[0]
        if "." in name:
            stem, _ = _split_name(name)
            return stem
        return name
    except Exception:
        return "video"


def _try_head_filename(url: str, timeout: int = 15) -> Optional[str]:
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout)
        cd = r.headers.get("Content-Disposition") or r.headers.get("content-disposition")
        if cd:
            m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cd)
            if m:
                return m.group(1)
    except Exception:
        pass
    return None


async def is_bot_admin(client, chat_id: int) -> bool:
    try:
        chat: Chat = await client.get_chat(chat_id)
        if chat.type in ["group", "supergroup"]:
            bot_member = await client.get_chat_member(chat_id, "me")
            return bot_member.status in ["administrator", "creator"]
        return False
    except Exception:
        return False


async def can_process_message(client, message: Message) -> bool:
    try:
        await client.get_chat(message.chat.id)
        if not await is_bot_admin(client, message.chat.id):
            return False
        if not _is_video_message(message):
            return False
        return True
    except (UserNotParticipant, ChatAdminRequired):
        return False
    except Exception:
        return False


def _bunny_headers_json() -> Dict[str, str]:
    if not BUNNY_API_KEY:
        raise RuntimeError("BUNNY_API_KEY is missing.")
    return {"AccessKey": BUNNY_API_KEY, "Content-Type": "application/json"}


def bunny_create_video(title: str) -> Dict[str, Any]:
    url = f"{BUNNY_API_BASE}/library/{BUNNY_LIBRARY_ID}/videos"
    r = requests.post(url, headers=_bunny_headers_json(), json={"title": title}, timeout=30)
    r.raise_for_status()
    return r.json()


def bunny_update_title(video_guid: str, title: str) -> Dict[str, Any]:
    url = f"{BUNNY_API_BASE}/library/{BUNNY_LIBRARY_ID}/videos/{video_guid}"
    r = requests.patch(url, headers=_bunny_headers_json(), json={"title": title}, timeout=20)
    r.raise_for_status()
    return r.json()


def bunny_fetch_from_url(video_guid: str, source_url: str) -> Dict[str, Any]:
    url = f"{BUNNY_API_BASE}/library/{BUNNY_LIBRARY_ID}/videos/{video_guid}/fetch"
    r = requests.post(url, headers=_bunny_headers_json(), json={"url": source_url}, timeout=30)
    r.raise_for_status()
    return r.json()


def bunny_upload_file(video_guid: str, local_path: str) -> None:
    url = f"{BUNNY_API_BASE}/library/{BUNNY_LIBRARY_ID}/videos/{video_guid}"
    headers = {"AccessKey": BUNNY_API_KEY, "Content-Type": "application/octet-stream"}
    with open(local_path, "rb") as f:
        r = requests.put(url, headers=headers, data=f, timeout=1800)
    r.raise_for_status()


def bunny_get_video(video_guid: str) -> Dict[str, Any]:
    url = f"{BUNNY_API_BASE}/library/{BUNNY_LIBRARY_ID}/videos/{video_guid}"
    r = requests.get(url, headers={"AccessKey": BUNNY_API_KEY}, timeout=20)
    r.raise_for_status()
    return r.json()


def bunny_list_videos(page: int = 1, per_page: int = 100) -> Dict[str, Any]:
    url = f"{BUNNY_API_BASE}/library/{BUNNY_LIBRARY_ID}/videos?page={page}&itemsPerPage={per_page}"
    r = requests.get(url, headers={"AccessKey": BUNNY_API_KEY}, timeout=30)
    r.raise_for_status()
    return r.json()


def bunny_delete_video(video_guid: str) -> None:
    url = f"{BUNNY_API_BASE}/library/{BUNNY_LIBRARY_ID}/videos/{video_guid}"
    r = requests.delete(url, headers={"AccessKey": BUNNY_API_KEY}, timeout=20)
    r.raise_for_status()


def _parse_available_resolutions(info: Dict[str, Any]) -> Set[int]:
    av = info.get("availableResolutions")
    s: Set[int] = set()
    if isinstance(av, str):
        for x in av.split(","):
            x = x.strip().lower()
            if x.endswith("p"):
                try:
                    s.add(int(x[:-1]))
                except Exception:
                    pass
    return s


def _status_str(info: Dict[str, Any]) -> str:
    ep = info.get("encodeProgress")
    st = info.get("status")
    return f"status={st}, encodeProgress={ep}%"


async def wait_until_ready_full(
    video_guid: str,
    status_msg: Optional[Message] = None,
    base_name: Optional[str] = None,
    timeout_s: int = WAIT_TIMEOUT_S,
    interval_s: int = WAIT_INTERVAL_S,
    stop_on_first_available: bool = False,
) -> Set[int]:
    loop = asyncio.get_running_loop()
    start = loop.time()
    last_text = None
    seen: Set[int] = set()
    while loop.time() - start < timeout_s:
        info = await asyncio.to_thread(bunny_get_video, video_guid)
        avail = _parse_available_resolutions(info).intersection(ALLOWED_RES)
        if avail:
            seen = avail
        if status_msg is not None:
            lines = [f"⏳ جارٍ المعالجة على Bunny ... ({_status_str(info)})"]
            if avail:
                stem, ext = _split_name(base_name or f"{video_guid}.mp4")
                preview = [
                    f"https://{BUNNY_CDN_HOST}/{video_guid}/play_{r}p.mp4 -n {_replace_or_append_res_with_ext(stem, r, ext)}"
                    for r in sorted(avail)
                ]
                lines.append("🔹 جودات ظهرت حتى الآن:")
                lines.extend(preview)
            new_text = "\n".join(lines)
            if new_text != last_text:
                await safe_edit(status_msg, new_text)
                last_text = new_text
        if stop_on_first_available and avail:
            return avail
        ep = info.get("encodeProgress")
        st = info.get("status")
        if (
            (isinstance(ep, int) and ep >= 100)
            or (isinstance(st, int) and st >= 4)
            or (isinstance(st, str) and st.lower() in {"encoded", "ready", "finished"})
        ):
            final_info = await asyncio.to_thread(bunny_get_video, video_guid)
            final_avail = _parse_available_resolutions(final_info).intersection(ALLOWED_RES)
            return final_avail or seen
        await asyncio.sleep(interval_s)
    return seen


def build_bunny_links(video_guid: str, base_name: str, res_list: List[int]) -> List[str]:
    lines = []
    stem, ext = _split_name(base_name)
    for r in res_list:
        url = f"https://{BUNNY_CDN_HOST}/{video_guid}/play_{r}p.mp4"
        out_name = _replace_or_append_res_with_ext(stem, r, ext)
        lines.append(f"{url} -n {out_name}")
    return lines


def _pg_conn():
    if not POSTGRES_URI:
        raise RuntimeError("POSTGRES_URI is not set (required for de-dup).")
    return psycopg2.connect(POSTGRES_URI)


def ensure_tables():
    conn = _pg_conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bunny_imports (
            source_url TEXT PRIMARY KEY,
            video_guid TEXT NOT NULL,
            title TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """
    )
    conn.commit()
    cur.close()
    conn.close()


def db_get_guid_by_url(source_url: str) -> Optional[str]:
    conn = _pg_conn()
    cur = conn.cursor()
    cur.execute("SELECT video_guid FROM bunny_imports WHERE source_url=%s", (source_url,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None


def db_insert_import(source_url: str, video_guid: str, title: str) -> None:
    conn = _pg_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO bunny_imports (source_url, video_guid, title) VALUES (%s,%s,%s)",
            (source_url, video_guid, title),
        )
        conn.commit()
    except errors.UniqueViolation:
        conn.rollback()
    finally:
        cur.close()
        conn.close()


async def process_single_video(client, item: VideoQueueItem) -> bool:
    try:
        status = await client.send_message(item.chat_id, f"⬇️ جاري تنزيل الفيديو: {item.file_name}")
        async with _import_semaphore:
            with tempfile.TemporaryDirectory() as tmpdir:
                stem, ext = _split_name(item.file_name)
                local_path = os.path.join(tmpdir, f"in_{item.file_unique_id}{_ensure_mp4_ext(ext)}")
                await client.download_media(item.message, file_name=local_path)
                display_title = f"{stem}{_ensure_mp4_ext(ext)}"
                status = await safe_edit(status, f"📄 إنشاء فيديو على Bunny: {display_title}")
                created = await asyncio.to_thread(bunny_create_video, display_title)
                video_guid = created.get("guid") or created.get("videoGuid") or created.get("guidId")
                if not video_guid:
                    await safe_edit(status, f"❌ فشل إنشاء الفيديو: {display_title}")
                    return False
                try:
                    await asyncio.to_thread(bunny_update_title, video_guid, display_title)
                except Exception:
                    pass
                status = await safe_edit(status, f"📤 رفع الملف: {display_title}")
                await asyncio.to_thread(bunny_upload_file, video_guid, local_path)
                status = await safe_edit(status, f"⏳ انتظار توفر أول جودة: {display_title}")
                available = await wait_until_ready_full(
                    video_guid,
                    status_msg=status,
                    base_name=display_title,
                    timeout_s=WAIT_TIMEOUT_S,
                    interval_s=WAIT_INTERVAL_S,
                    stop_on_first_available=True,
                )
                if not available:
                    await safe_edit(status, f"⚠️ لا توجد جودات متاحة: {display_title}")
                    return False
                lines = build_bunny_links(video_guid, display_title, sorted(available))
                await safe_edit(status, f"✅ تم الانتهاء من: {display_title}\n\n" + "\n".join(lines))
                return True
    except Exception as e:
        try:
            await client.send_message(item.chat_id, f"❌ خطأ في معالجة {item.file_name}: {str(e)}")
        except Exception:
            pass
        return False


async def queue_processor(client):
    global is_processing
    while True:
        try:
            item = await video_queue.get()
            is_processing = True
            await client.send_message(
                item.chat_id,
                f"🔄 بدء معالجة الفيديو: {item.file_name}\n📊 متبقي في القائمة: {video_queue.qsize()}",
            )
            success = await process_single_video(client, item)
            remaining = video_queue.qsize()
            if remaining > 0:
                await client.send_message(
                    item.chat_id,
                    f"📈 تم الانتهاء من: {item.file_name}\n⏭️ الانتقال للفيديو التالي...\n📊 متبقي: {remaining}",
                )
            else:
                await client.send_message(
                    item.chat_id,
                    f"🎉 تم الانتهاء من جميع الفيديوهات!\n✅ آخر فيديو: {item.file_name}",
                )
                is_processing = False
            video_queue.task_done()
        except Exception:
            is_processing = False
            await asyncio.sleep(5)


async def start_queue_processor(client):
    asyncio.create_task(queue_processor(client))


async def auto_process_video(client, message: Message):
    global processed_videos
    if not (BUNNY_API_KEY and BUNNY_LIBRARY_ID and BUNNY_CDN_HOST):
        return
    if not auto_process_enabled:
        return
    if not await can_process_message(client, message):
        return
    media = message.video or message.document
    file_unique_id = media.file_unique_id
    if file_unique_id in processed_videos:
        return
    filename = getattr(media, "file_name", None) or f"video_{message.id}.mp4"
    if not is_processing and video_queue.empty():
        await start_queue_processor(client)
    queue_item = VideoQueueItem(
        message=message,
        file_name=filename,
        file_unique_id=file_unique_id,
        chat_id=message.chat.id,
    )
    await video_queue.put(queue_item)
    processed_videos.add(file_unique_id)
    total_in_queue = video_queue.qsize()
    status_text = "✅ تم إضافة الفيديو لقائمة المعالجة تلقائيًا\n"
    status_text += f"📊 إجمالي الفيديوهات في القائمة: {total_in_queue}\n"
    status_text += "⚡ المعالجة جارية حالياً..." if is_processing else "🚀 سيبدأ المعالجة خلال ثوانٍ..."
    try:
        await message.reply_text(status_text)
    except Exception:
        pass


async def cmd_upload_from_telegram(client, message: Message):
    if not (BUNNY_API_KEY and BUNNY_LIBRARY_ID and BUNNY_CDN_HOST):
        await message.reply_text("⚠️ إعدادات Bunny ناقصة (API KEY / LIBRARY ID / CDN HOST).")
        return
    count = 1
    args = message.command[1:]
    if args:
        if args[0] == "-i" and len(args) >= 2:
            try:
                count = int(args[1])
            except ValueError:
                await message.reply_text("❌ عدد غير صحيح بعد -i")
                return
        else:
            try:
                count = int(args[0])
            except ValueError:
                await message.reply_text("❌ عدد غير صحيح")
                return
    count = max(1, min(50, count))
    replied = message.reply_to_message
    videos_to_process = []
    if not replied:
        await message.reply_text(
            "↩️ من فضلك ردّ بالأمر /c على رسالة تحتوي فيديو.\n"
            "💡 يمكنك استخدام `/c -i 5` لمعالجة 5 فيديوهات من النقطة المحددة."
        )
        return
    current_msg = replied
    for _ in range(count):
        if not current_msg:
            break
        if _is_video_message(current_msg):
            media = current_msg.video or current_msg.document
            filename = getattr(media, "file_name", None) or f"video_{current_msg.id}.mp4"
            videos_to_process.append(
                {
                    "message": current_msg,
                    "filename": filename,
                    "file_unique_id": media.file_unique_id,
                }
            )
        try:
            next_msg = await client.get_messages(
                chat_id=message.chat.id, message_ids=current_msg.id + 1
            )
            current_msg = next_msg if next_msg else None
        except Exception:
            current_msg = None
    if not videos_to_process:
        await message.reply_text("❌ لم يتم العثور على فيديوهات للمعالجة.")
        return
    if not is_processing and video_queue.empty():
        await start_queue_processor(client)
    added_count = 0
    for video_info in videos_to_process:
        queue_item = VideoQueueItem(
            message=video_info["message"],
            file_name=video_info["filename"],
            file_unique_id=video_info["file_unique_id"],
            chat_id=message.chat.id,
        )
        await video_queue.put(queue_item)
        added_count += 1
    total_in_queue = video_queue.qsize()
    status_text = f"✅ تم إضافة {added_count} فيديو لقائمة المعالجة\n"
    status_text += f"📊 إجمالي الفيديوهات في القائمة: {total_in_queue}\n"
    status_text += "⚡ المعالجة جارية حالياً..." if is_processing else "🚀 سيبدأ المعالجة خلال ثوانٍ..."
    await message.reply_text(status_text)


async def cmd_queue_status(client, message: Message):
    queue_size = video_queue.qsize()
    status = "🔄 جاري المعالجة" if is_processing else "⏸️ متوقف"
    await message.reply_text(
        f"📊 حالة قائمة المعالجة:\n"
        f"📈 عدد الفيديوهات المتبقية: {queue_size}\n"
        f"⚡ الحالة: {status}"
    )


async def cmd_link_import(client, message: Message):
    if not (BUNNY_API_KEY and BUNNY_LIBRARY_ID and BUNNY_CDN_HOST and POSTGRES_URI):
        await message.reply_text("⚠️ إعدادات ناقصة: تأكد من BUNNY_* و POSTGRES_URI.")
        return
    if len(message.command) < 2:
        await message.reply_text("❌ استخدم:\n`/l http://example.com/video.mp4`")
        return
    source_url = message.command[1].strip()
    note = await message.reply_text("☁️ بدء الاستيراد من الرابط ...")
    async with _import_semaphore:
        real_name = await asyncio.to_thread(_try_head_filename, source_url)
        if real_name:
            stem, ext = _split_name(real_name)
            base_name = f"{stem}{_ensure_mp4_ext(ext)}"
            display_title = base_name
        else:
            base_name = f"{_derive_title_from_url(source_url)}.mp4"
            display_title = base_name
        try:
            ensure_tables()
            existing_guid = db_get_guid_by_url(source_url)
            if existing_guid:
                try:
                    info = await asyncio.to_thread(bunny_get_video, existing_guid)
                    current_title = (info.get("title") or "").strip()
                    if current_title != display_title:
                        await asyncio.to_thread(
                            bunny_update_title, existing_guid, display_title
                        )
                except Exception:
                    pass
                await safe_edit(
                    note, "ℹ️ الفيديو موجود مسبقًا، سيتم الانتظار حتى اكتمال التجهيز ..."
                )
                available = await wait_until_ready_full(
                    existing_guid,
                    status_msg=note,
                    base_name=base_name,
                    timeout_s=WAIT_TIMEOUT_S,
                    interval_s=WAIT_INTERVAL_S,
                )
                if not available:
                    await safe_edit(note, "⚠️ انتهت المهلة بدون جودات. جرّب لاحقًا.")
                    return
                lines = build_bunny_links(existing_guid, base_name, sorted(available))
                await safe_edit(note, "\n".join(lines))
                return
            await safe_edit(note, "📄 إنشاء فيديو على Bunny Stream ...")
            created = await asyncio.to_thread(bunny_create_video, display_title)
            video_guid = created.get("guid") or created.get("videoGuid") or created.get("guidId")
            if not video_guid:
                await safe_edit(note, f"❌ لم يتم استلام GUID من Bunny: {created}")
                return
            db_insert_import(source_url, video_guid, display_title)
            try:
                await asyncio.to_thread(bunny_update_title, video_guid, display_title)
            except Exception:
                pass
            await asyncio.to_thread(bunny_fetch_from_url, video_guid, source_url)
            await safe_edit(note, "📥 تم البدء، سيتم الانتظار حتى اكتمال المعالجة ...")
            available = await wait_until_ready_full(
                video_guid,
                status_msg=note,
                base_name=base_name,
                timeout_s=WAIT_TIMEOUT_S,
                interval_s=WAIT_INTERVAL_S,
            )
            if not available:
                await safe_edit(note, "⚠️ انتهت المهلة بدون جودات. جرّب لاحقًا.")
                return
            lines = build_bunny_links(video_guid, base_name, sorted(available))
            await safe_edit(note, "\n".join(lines))
        except requests.HTTPError as e:
            try:
                await safe_edit(note, f"❌ خطأ HTTP: {e} | {getattr(e.response, 'text', '')}")
            except Exception:
                pass
        except Exception as e:
            if "MESSAGE_NOT_MODIFIED" in str(e):
                return
            try:
                await safe_edit(note, f"❌ حدث خطأ غير متوقع: {e}")
            except Exception:
                pass


async def cmd_from_iframe(client, message: Message):
    if len(message.command) < 2:
        await message.reply_text(
            "❌ استخدم:\n`/ll https://iframe.mediadelivery.net/play/<LIB_ID>/<VIDEO_GUID>`"
        )
        return
    url = message.command[1].strip()
    m = re.search(r"mediadelivery\.net/play/(\d+)/([a-f0-9\-]{36})", url, re.IGNORECASE)
    if not m:
        await message.reply_text(
            "⚠️ رابط غير صحيح. رجاءً أرسل لينك من شكل iframe.mediadelivery.net/play/<LIB>/<GUID>"
        )
        return
    video_guid = m.group(2)
    note = await message.reply_text("🔎 فحص الجودات المتاحة ...")
    try:
        info = await asyncio.to_thread(bunny_get_video, video_guid)
        title = (info.get("title") or f"{video_guid}.mp4").strip()
        base_name = title if title.lower().endswith(".mp4") else f"{title}.mp4"
        available = _parse_available_resolutions(info).intersection(ALLOWED_RES)
        if not available:
            await safe_edit(note, "⚠️ لا توجد جودات متاحة الآن لهذا الفيديو.")
            return
        lines = build_bunny_links(video_guid, base_name, sorted(available))
        await safe_edit(note, "\n".join(lines))
    except requests.HTTPError as e:
        await safe_edit(note, f"❌ خطأ HTTP: {e} | {getattr(e.response, 'text', '')}")
    except Exception as e:
        if "MESSAGE_NOT_MODIFIED" in str(e):
            return
        await safe_edit(note, f"❌ حدث خطأ غير متوقع: {e}")


async def cmd_list_all_links(client, message: Message):
    note = await message.reply_text("📦 جارٍ تجميع روابط كل الفيديوهات ...")
    try:
        all_items: List[Dict[str, Any]] = []
        page = 1
        while True:
            data = await asyncio.to_thread(bunny_list_videos, page, 100)
            items = data.get("items") or data.get("videos") or []
            if not items:
                break
            all_items.extend(items)
            total = data.get("totalItems") or 0
            if page * 100 >= total or not total:
                break
            page += 1
        if not all_items:
            await safe_edit(note, "ℹ️ لا توجد فيديوهات في المكتبة حالياً.")
            return
        blocks: List[Tuple[str, List[str]]] = []
        for it in all_items:
            guid = it.get("guid") or it.get("videoGuid")
            if not guid:
                continue
            info = await asyncio.to_thread(bunny_get_video, guid)
            title = (info.get("title") or f"{guid}.mp4").strip()
            base_name = title if title.lower().endswith(".mp4") else f"{title}.mp4"
            available = _parse_available_resolutions(info).intersection(ALLOWED_RES)
            if not available:
                continue
            links = build_bunny_links(guid, base_name, sorted(available))
            blocks.append((title.lower(), links))
        if not blocks:
            await safe_edit(note, "ℹ️ لا توجد جودات متاحة لأي فيديو حالياً.")
            return
        blocks.sort(key=lambda x: x[0])
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = os.path.join(tmpdir, "bunny_links.txt")
            with open(out_path, "w", encoding="utf-8") as f:
                for _, links in blocks:
                    for line in links:
                        f.write(line + "\n")
                    f.write("\n")
            await message.reply_document(out_path, caption="✅ تم إنشاء ملف الروابط لجميع الفيديوهات.")
        await note.delete()
    except requests.HTTPError as e:
        await safe_edit(note, f"❌ خطأ HTTP: {e} | {getattr(e.response, 'text', '')}")
    except Exception as e:
        if "MESSAGE_NOT_MODIFIED" in str(e):
            return
        await safe_edit(note, f"❌ حدث خطأ غير متوقع: {e}")


async def bunny_delete_all(client, message: Message):
    if not BUNNY_API_KEY or not BUNNY_LIBRARY_ID:
        await message.reply_text("⚠️ إعدادات Bunny ناقصة.")
        return
    note = await message.reply_text("🗑️ جارٍ جلب الفيديوهات من Bunny ...")
    try:
        total_deleted = 0
        page = 1
        while True:
            data = await asyncio.to_thread(bunny_list_videos, page, 100)
            items = data.get("items") or data.get("videos") or []
            if not items:
                break
            for it in items:
                guid = it.get("guid") or it.get("videoGuid")
                if not guid:
                    continue
                try:
                    await asyncio.to_thread(bunny_delete_video, guid)
                    total_deleted += 1
                except Exception as e:
                    await message.reply_text(f"⚠️ فشل حذف {guid}: {e}")
            total = data.get("totalItems") or 0
            if page * 100 >= total or not total:
                break
            page += 1
        await safe_edit(note, f"✅ تم حذف {total_deleted} فيديو من Bunny Stream.")
    except requests.HTTPError as e:
        await safe_edit(note, f"❌ خطأ HTTP: {e} | {getattr(e.response, 'text', '')}")
    except Exception as e:
        if "MESSAGE_NOT_MODIFIED" in str(e):
            return
        await safe_edit(note, f"❌ حدث خطأ غير متوقع: {e}")


async def cmd_library_stats(client, message: Message):
    if not BUNNY_API_KEY or not BUNNY_LIBRARY_ID:
        await message.reply_text("⚠️ إعدادات Bunny ناقصة.")
        return
    note = await message.reply_text("📊 جاري جمع الإحصائيات...")
    try:
        data = await asyncio.to_thread(bunny_list_videos, 1, 1)
        total_videos = data.get("totalItems", 0)
        resolution_count = {360: 0, 480: 0, 720: 0, 1080: 0}
        processed_sample = 0
        sample_data = await asyncio.to_thread(bunny_list_videos, 1, 20)
        sample_items = sample_data.get("items", [])
        for item in sample_items:
            guid = item.get("guid") or item.get("videoGuid")
            if guid:
                try:
                    info = await asyncio.to_thread(bunny_get_video, guid)
                    available = _parse_available_resolutions(info).intersection(ALLOWED_RES)
                    for res in available:
                        resolution_count[res] += 1
                    processed_sample += 1
                except Exception:
                    continue
        stats_text = (
            "📊 **إحصائيات مكتبة Bunny Stream**\n"
            f"🎬 **إجمالي الفيديوهات:** {total_videos}\n"
            f"📋 **تم فحص عينة:** {processed_sample} فيديو\n"
            "📺 **الجودات المتاحة (في العينة):**\n"
            f"• 360p: {resolution_count[360]} فيديو\n"
            f"• 480p: {resolution_count[480]} فيديو  \n"
            f"• 720p: {resolution_count[720]} فيديو\n"
            f"• 1080p: {resolution_count[1080]} فيديو\n"
            "📊 **حالة القائمة الحالية:**\n"
            f"• في الانتظار: {video_queue.qsize()} فيديو\n"
            f"• الحالة: {'🔄 نشط' if is_processing else '⏸️ متوقف'}"
        )
        await safe_edit(note, stats_text)
    except Exception as e:
        await safe_edit(note, f"❌ خطأ في جمع الإحصائيات: {e}")


async def cmd_test_connection(client, message: Message):
    if not BUNNY_API_KEY or not BUNNY_LIBRARY_ID:
        await message.reply_text("⚠️ إعدادات Bunny ناقصة.")
        return
    note = await message.reply_text("🔄 اختبار الاتصال...")
    try:
        data = await asyncio.to_thread(bunny_list_videos, 1, 1)
        total = data.get("totalItems", 0)
        await safe_edit(note, f"✅ الاتصال سليم!\n📊 المكتبة تحتوي على {total} فيديو.")
    except requests.HTTPError as e:
        await safe_edit(note, f"❌ خطأ في الاتصال: {e}")
    except Exception as e:
        await safe_edit(note, f"❌ خطأ غير متوقع: {e}")


async def cmd_stop_queue(client, message: Message):
    global is_processing
    if is_processing:
        is_processing = False
        await message.reply_text("⏹️ تم إيقاف معالجة القائمة مؤقتاً.")
    else:
        await message.reply_text("ℹ️ المعالجة متوقفة بالفعل.")


async def cmd_resume_queue(client, message: Message):
    global is_processing
    if not is_processing and not video_queue.empty():
        await start_queue_processor(client)
        await message.reply_text("▶️ تم استئناف معالجة القائمة.")
    elif is_processing:
        await message.reply_text("ℹ️ المعالجة تعمل بالفعل.")
    else:
        await message.reply_text("ℹ️ لا توجد فيديوهات في القائمة.")


async def cmd_clear_queue(client, message: Message):
    global is_processing
    cleared_count = video_queue.qsize()
    while not video_queue.empty():
        try:
            video_queue.get_nowait()
            video_queue.task_done()
        except asyncio.QueueEmpty:
            break
    is_processing = False
    await message.reply_text(f"🗑️ تم مسح {cleared_count} فيديو من القائمة.")


async def cmd_toggle_auto(client, message: Message):
    global auto_process_enabled
    if len(message.command) < 2:
        await message.reply_text(
            "❌ استخدم:\n`/auto on` لتفعيل المعالجة التلقائية\n`/auto off` لتعطيلها"
        )
        return
    option = message.command[1].lower()
    if option == "on":
        auto_process_enabled = True
        await message.reply_text("✅ تم تفعيل المعالجة التلقائية للفيديوهات")
    elif option == "off":
        auto_process_enabled = False
        await message.reply_text("❌ تم تعطيل المعالجة التلقائية للفيديوهات")
    else:
        await message.reply_text("❌ خيار غير صحيح. استخدم `on` أو `off`")


COMMAND_HANDLERS = [
    MessageHandler(auto_process_video, filters.group & (filters.video | filters.document)),
    MessageHandler(
        cmd_upload_from_telegram,
        filters.command(_bunny_command("c")) & (filters.private | filters.group | filters.channel),
    ),
    MessageHandler(
        cmd_queue_status,
        filters.command(_bunny_command("queue")) & (filters.private | filters.group | filters.channel),
    ),
    MessageHandler(
        cmd_link_import,
        filters.command(_bunny_command("l")) & (filters.private | filters.group | filters.channel),
    ),
    MessageHandler(
        cmd_from_iframe,
        filters.command(_bunny_command("ll")) & (filters.private | filters.group | filters.channel),
    ),
    MessageHandler(
        cmd_list_all_links,
        filters.command(_bunny_command("ls")) & (filters.private | filters.group | filters.channel),
    ),
    MessageHandler(
        bunny_delete_all,
        filters.command(_bunny_command("dc")) & (filters.private | filters.group),
    ),
    MessageHandler(
        cmd_library_stats,
        filters.command(_bunny_command("stats")) & (filters.private | filters.group | filters.channel),
    ),
    MessageHandler(
        cmd_test_connection,
        filters.command(_bunny_command("test")) & (filters.private | filters.group),
    ),
    MessageHandler(
        cmd_stop_queue,
        filters.command(_bunny_command("stop")) & (filters.private | filters.group),
    ),
    MessageHandler(
        cmd_resume_queue,
        filters.command(_bunny_command("resume")) & (filters.private | filters.group),
    ),
    MessageHandler(
        cmd_clear_queue,
        filters.command(_bunny_command("clear")) & (filters.private | filters.group),
    ),
    MessageHandler(
        cmd_toggle_auto,
        filters.command(_bunny_command("auto")) & (filters.private | filters.group),
    ),
]

for handler in COMMAND_HANDLERS:
    bot.add_handler(handler)

