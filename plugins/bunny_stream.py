"""Bunny Stream integration plugin."""

from __future__ import annotations

import asyncio
import os
import re
import tempfile
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse

import requests
from pyrogram import Client, filters
from pyrogram.errors import ChatAdminRequired, MessageNotModified, RPCError, UserNotParticipant
from pyrogram.handlers import MessageHandler
from pyrogram.types import Chat, Message

from bot import LOGGER
from bot.core.config_manager import Config
from bot.core.plugin_manager import PluginBase, PluginInfo, get_plugin_manager
from bot.helper.ext_utils.bot_utils import new_task
from bot.helper.telegram_helper.filters import CustomFilters

try:
    import psycopg2
    from psycopg2 import errors
except Exception:  # pragma: no cover - optional dependency
    psycopg2 = None
    errors = None


BUNNY_API_BASE = "https://video.bunnycdn.com"


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_resolutions(value: Any, default: Iterable[int]) -> List[int]:
    if isinstance(value, (list, tuple, set)):
        cleaned = []
        for item in value:
            try:
                cleaned.append(int(item))
            except (TypeError, ValueError):
                continue
        return cleaned or list(default)
    if isinstance(value, str):
        parts = re.split(r"[\s,]+", value.strip())
        cleaned = []
        for item in parts:
            if not item:
                continue
            item = item.rstrip("pP")
            try:
                cleaned.append(int(item))
            except ValueError:
                continue
        return cleaned or list(default)
    return list(default)


BUNNY_LIBRARY_ID = _to_int(Config.BUNNY_LIBRARY_ID, 0)
BUNNY_API_KEY = (Config.BUNNY_API_KEY or "").strip()
BUNNY_CDN_HOST = (Config.BUNNY_CDN_HOST or "").strip()
MAX_PARALLEL_IMPORTS = max(1, _to_int(Config.BUNNY_MAX_PARALLEL_IMPORTS, 4))
WAIT_TIMEOUT_S = _to_int(Config.BUNNY_WAIT_TIMEOUT_S, 7200)
WAIT_INTERVAL_S = _to_int(Config.BUNNY_WAIT_INTERVAL_S, 30)
ALLOWED_RES = _normalize_resolutions(Config.BUNNY_ALLOWED_RES, [360, 480, 720, 1080])
COMMAND_PREFIX = (Config.BUNNY_COMMAND_PREFIX or "").strip()
COMMAND_ALIASES_RAW = Config.BUNNY_COMMAND_ALIASES or "c:coo,l:li"
DEFAULT_AUTO_STATE = bool(Config.BUNNY_AUTO_PROCESS_ENABLED)
BUNNY_DATABASE_URL = (Config.BUNNY_DATABASE_URL or "").strip()

_import_semaphore = asyncio.Semaphore(MAX_PARALLEL_IMPORTS)


def _parse_alias_map(value: Any) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if isinstance(value, dict):
        for key, alias in value.items():
            if key and alias:
                mapping[str(key).strip()] = str(alias).strip()
        return mapping
    if isinstance(value, str):
        for part in value.split(","):
            part = part.strip()
            if not part or ":" not in part:
                continue
            base, alias = part.split(":", 1)
            base = base.strip()
            alias = alias.strip()
            if base and alias:
                mapping[base] = alias
    return mapping


COMMAND_ALIASES = _parse_alias_map(COMMAND_ALIASES_RAW)


def _command_name(base: str) -> str:
    base = base.strip()
    if base in COMMAND_ALIASES:
        return COMMAND_ALIASES[base]
    return f"{COMMAND_PREFIX}{base}" if COMMAND_PREFIX else base


def _base_from_invoked(invoked: str) -> str:
    for base, alias in COMMAND_ALIASES.items():
        if alias == invoked:
            return base
    if COMMAND_PREFIX and invoked.startswith(COMMAND_PREFIX):
        return invoked[len(COMMAND_PREFIX) :]
    return invoked


COMMAND_BASES = [
    "c",
    "queue",
    "auto",
    "stop",
    "resume",
    "clear",
    "l",
    "ll",
    "ls",
    "dc",
    "stats",
    "test",
]


def _command_triggers() -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for base in COMMAND_BASES:
        trigger = _command_name(base) or base
        mapping[base] = trigger
    return mapping


COMMAND_TRIGGER_MAP = _command_triggers()


def _registered_command_names() -> List[str]:
    names: Set[str] = set(COMMAND_BASES)
    for trigger in COMMAND_TRIGGER_MAP.values():
        if trigger:
            names.add(trigger)
    return sorted(names)


def _db_available() -> bool:
    return bool(BUNNY_DATABASE_URL and psycopg2 is not None)


@dataclass
class VideoQueueItem:
    message: Message
    file_name: str
    file_unique_id: str
    chat_id: int


video_queue: asyncio.Queue[VideoQueueItem] = asyncio.Queue()
processed_videos: Set[str] = set()
is_processing: bool = False
auto_process_enabled: bool = DEFAULT_AUTO_STATE
queue_processor_task: Optional[asyncio.Task] = None
processor_client: Optional[Client] = None
queue_pause_event = asyncio.Event()
queue_pause_event.set()


def _is_video_message(msg: Optional[Message]) -> bool:
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
        return stem, f".{ext}"
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
    except RPCError:
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
        response = requests.head(url, allow_redirects=True, timeout=timeout)
        content_disposition = response.headers.get("Content-Disposition") or response.headers.get(
            "content-disposition"
        )
        if content_disposition:
            match = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', content_disposition)
            if match:
                return match.group(1)
    except Exception:
        return None
    return None


async def is_bot_admin(client: Client, chat_id: int) -> bool:
    try:
        chat: Chat = await client.get_chat(chat_id)
        if chat.type in {"group", "supergroup"}:
            bot_member = await client.get_chat_member(chat_id, "me")
            return bot_member.status in {"administrator", "creator"}
        return False
    except Exception:
        return False


async def can_process_message(client: Client, message: Message) -> bool:
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
    response = requests.post(url, headers=_bunny_headers_json(), json={"title": title}, timeout=30)
    response.raise_for_status()
    return response.json()


def bunny_update_title(video_guid: str, title: str) -> Dict[str, Any]:
    url = f"{BUNNY_API_BASE}/library/{BUNNY_LIBRARY_ID}/videos/{video_guid}"
    response = requests.patch(url, headers=_bunny_headers_json(), json={"title": title}, timeout=20)
    response.raise_for_status()
    return response.json()


def bunny_fetch_from_url(video_guid: str, source_url: str) -> Dict[str, Any]:
    url = f"{BUNNY_API_BASE}/library/{BUNNY_LIBRARY_ID}/videos/{video_guid}/fetch"
    response = requests.post(url, headers=_bunny_headers_json(), json={"url": source_url}, timeout=30)
    response.raise_for_status()
    return response.json()


def bunny_upload_file(video_guid: str, local_path: str) -> None:
    url = f"{BUNNY_API_BASE}/library/{BUNNY_LIBRARY_ID}/videos/{video_guid}"
    headers = {"AccessKey": BUNNY_API_KEY, "Content-Type": "application/octet-stream"}
    with open(local_path, "rb") as file_obj:
        response = requests.put(url, headers=headers, data=file_obj, timeout=1800)
    response.raise_for_status()


def bunny_get_video(video_guid: str) -> Dict[str, Any]:
    url = f"{BUNNY_API_BASE}/library/{BUNNY_LIBRARY_ID}/videos/{video_guid}"
    response = requests.get(url, headers={"AccessKey": BUNNY_API_KEY}, timeout=20)
    response.raise_for_status()
    return response.json()


def bunny_list_videos(page: int = 1, per_page: int = 100) -> Dict[str, Any]:
    url = f"{BUNNY_API_BASE}/library/{BUNNY_LIBRARY_ID}/videos?page={page}&itemsPerPage={per_page}"
    response = requests.get(url, headers={"AccessKey": BUNNY_API_KEY}, timeout=30)
    response.raise_for_status()
    return response.json()


def bunny_delete_video(video_guid: str) -> None:
    url = f"{BUNNY_API_BASE}/library/{BUNNY_LIBRARY_ID}/videos/{video_guid}"
    response = requests.delete(url, headers={"AccessKey": BUNNY_API_KEY}, timeout=20)
    response.raise_for_status()


def _parse_available_resolutions(info: Dict[str, Any]) -> Set[int]:
    available = info.get("availableResolutions")
    result: Set[int] = set()
    if isinstance(available, str):
        for chunk in available.split(","):
            chunk = chunk.strip().lower()
            if chunk.endswith("p"):
                try:
                    result.add(int(chunk[:-1]))
                except ValueError:
                    continue
    return result


def _status_str(info: Dict[str, Any]) -> str:
    progress = info.get("encodeProgress")
    status = info.get("status")
    return f"status={status}, encodeProgress={progress}%"


async def wait_until_ready_full(
    video_guid: str,
    status_msg: Optional[Message] = None,
    base_name: Optional[str] = None,
    timeout_s: int = WAIT_TIMEOUT_S,
    interval_s: int = WAIT_INTERVAL_S,
) -> Set[int]:
    start = asyncio.get_event_loop().time()
    last_text: Optional[str] = None
    seen: Set[int] = set()
    while asyncio.get_event_loop().time() - start < timeout_s:
        info = await asyncio.to_thread(bunny_get_video, video_guid)
        available = _parse_available_resolutions(info).intersection(ALLOWED_RES)
        if available:
            seen = available
        if status_msg is not None:
            lines = [f"⏳ جارٍ المعالجة على Bunny ... ({_status_str(info)})"]
            if available:
                stem, ext = _split_name(base_name or f"{video_guid}.mp4")
                preview = [
                    f"https://{BUNNY_CDN_HOST}/{video_guid}/play_{res}p.mp4 -n "
                    f"{_replace_or_append_res_with_ext(stem, res, ext)}"
                    for res in sorted(available)
                ]
                lines.append("🔹 جودات ظهرت حتى الآن:")
                lines.extend(preview)
            new_text = "\n".join(lines)
            if new_text != last_text:
                await safe_edit(status_msg, new_text)
                last_text = new_text
        progress = info.get("encodeProgress")
        status = info.get("status")
        if (
            (isinstance(progress, int) and progress >= 100)
            or (isinstance(status, int) and status >= 4)
            or (isinstance(status, str) and status.lower() in {"encoded", "ready", "finished"})
        ):
            final_info = await asyncio.to_thread(bunny_get_video, video_guid)
            final_avail = _parse_available_resolutions(final_info).intersection(ALLOWED_RES)
            return final_avail or seen
        await asyncio.sleep(interval_s)
    return seen


def build_bunny_links(video_guid: str, base_name: str, res_list: Iterable[int]) -> List[str]:
    lines: List[str] = []
    stem, ext = _split_name(base_name)
    for res in res_list:
        url = f"https://{BUNNY_CDN_HOST}/{video_guid}/play_{res}p.mp4"
        out_name = _replace_or_append_res_with_ext(stem, res, ext)
        lines.append(f"{url} -n {out_name}")
    return lines


def _pg_conn():
    if not _db_available():
        raise RuntimeError("BUNNY_DATABASE_URL is not configured or psycopg2 is missing.")
    return psycopg2.connect(BUNNY_DATABASE_URL)


def ensure_tables():
    conn = _pg_conn()
    cur = conn.cursor()
    try:
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
    finally:
        cur.close()
        conn.close()


def db_get_guid_by_url(source_url: str) -> Optional[str]:
    conn = _pg_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT video_guid FROM bunny_imports WHERE source_url=%s", (source_url,))
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()
        conn.close()


def db_insert_import(source_url: str, video_guid: str, title: str) -> None:
    conn = _pg_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO bunny_imports (source_url, video_guid, title) VALUES (%s,%s,%s)",
            (source_url, video_guid, title),
        )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        if errors is not None and isinstance(exc, errors.UniqueViolation):  # pragma: no cover
            return
        raise
    finally:
        cur.close()
        conn.close()


async def process_single_video(client: Client, item: VideoQueueItem) -> bool:
    global is_processing
    try:
        status = await client.send_message(item.chat_id, f"⬇️ جاري تنزيل الفيديو: {item.file_name}")
        async with _import_semaphore:
            with tempfile.TemporaryDirectory() as tmpdir:
                stem, ext = _split_name(item.file_name)
                local_path = os.path.join(tmpdir, f"in_{item.file_unique_id}{_ensure_mp4_ext(ext)}")
                await client.download_media(item.message, file_name=local_path)
                display_title = f"{stem}{_ensure_mp4_ext(ext)}"
                await safe_edit(status, f"📄 إنشاء فيديو على Bunny: {display_title}")
                created = await asyncio.to_thread(bunny_create_video, display_title)
                video_guid = created.get("guid") or created.get("videoGuid") or created.get("guidId")
                if not video_guid:
                    await safe_edit(status, f"❌ فشل إنشاء الفيديو: {display_title}")
                    return False
                try:
                    await asyncio.to_thread(bunny_update_title, video_guid, display_title)
                except Exception:
                    pass
                await safe_edit(status, f"📤 رفع الملف: {display_title}")
                await asyncio.to_thread(bunny_upload_file, video_guid, local_path)
                await safe_edit(status, f"⏳ فحص الجودات: {display_title}")
                available = await wait_until_ready_full(
                    video_guid,
                    status_msg=status,
                    base_name=display_title,
                )
                if not available:
                    await safe_edit(status, f"⚠️ لا توجد جودات متاحة: {display_title}")
                    return False
                lines = build_bunny_links(video_guid, display_title, sorted(available))
                await safe_edit(status, f"✅ تم الانتهاء من: {display_title}\n\n" + "\n".join(lines))
                return True
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.error("Error processing video", exc_info=True)
        try:
            await client.send_message(item.chat_id, f"❌ خطأ في معالجة {item.file_name}: {exc}")
        except Exception:
            pass
        return False
    finally:
        if video_queue.empty():
            is_processing = False


async def queue_processor() -> None:
    global is_processing
    while True:
        await queue_pause_event.wait()
        item = await video_queue.get()
        client = processor_client
        if client is None:
            video_queue.task_done()
            await asyncio.sleep(1)
            continue
        try:
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
                    "📈 تم الانتهاء من: {name}\n⏭️ الانتقال للفيديو التالي...\n📊 متبقي: {remaining}".format(
                        name=item.file_name
                    ),
                )
            else:
                await client.send_message(
                    item.chat_id,
                    "🎉 تم الانتهاء من جميع الفيديوهات!\n✅ آخر فيديو: {name}".format(name=item.file_name),
                )
                is_processing = False
        except Exception:
            LOGGER.exception("Error in queue processor")
            await asyncio.sleep(5)
        finally:
            video_queue.task_done()


def ensure_queue_processor(client: Client) -> None:
    global queue_processor_task, processor_client
    processor_client = client
    if queue_processor_task is None or queue_processor_task.done():
        queue_processor_task = asyncio.create_task(queue_processor())


def _extract_command_args(message: Message) -> Tuple[str, List[str]]:
    parts = message.command or []
    if not parts:
        return "", []
    invoked = _base_from_invoked(parts[0])
    return invoked, parts[1:]


def _missing_config() -> bool:
    return not (BUNNY_API_KEY and BUNNY_LIBRARY_ID and BUNNY_CDN_HOST)


@new_task
async def c_command(client: Client, message: Message):
    if _missing_config():
        await message.reply_text("⚠️ إعدادات Bunny ناقصة (API KEY / LIBRARY ID / CDN HOST).")
        return
    _, args = _extract_command_args(message)
    count = 1
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
    if not replied:
        await message.reply_text(
            "↩️ من فضلك ردّ بالأمر /{cmd} على رسالة تحتوي فيديو.\n"
            "💡 يمكنك استخدام `/{cmd} -i 5` لمعالجة 5 فيديوهات من النقطة المحددة.".format(
                cmd=_command_name("c")
            )
        )
        return
    videos_to_process: List[VideoQueueItem] = []
    current_msg = replied
    for _ in range(count):
        if not current_msg:
            break
        if _is_video_message(current_msg):
            media = current_msg.video or current_msg.document
            filename = getattr(media, "file_name", None) or f"video_{current_msg.id}.mp4"
            videos_to_process.append(
                VideoQueueItem(
                    message=current_msg,
                    file_name=filename,
                    file_unique_id=media.file_unique_id,
                    chat_id=message.chat.id,
                )
            )
        try:
            next_msg = await client.get_messages(chat_id=message.chat.id, message_ids=current_msg.id + 1)
            current_msg = next_msg if next_msg else None
        except Exception:
            current_msg = None
    if not videos_to_process:
        await message.reply_text("❌ لم يتم العثور على فيديوهات للمعالجة.")
        return
    ensure_queue_processor(client)
    for item in videos_to_process:
        await video_queue.put(item)
        processed_videos.add(item.file_unique_id)
    total_in_queue = video_queue.qsize()
    status_lines = [
        f"✅ تم إضافة {len(videos_to_process)} فيديو لقائمة المعالجة",
        f"📊 إجمالي الفيديوهات في القائمة: {total_in_queue}",
    ]
    status_lines.append("⚡ المعالجة جارية حالياً..." if is_processing else "🚀 سيبدأ المعالجة خلال ثوانٍ...")
    await message.reply_text("\n".join(status_lines))


@new_task
async def queue_command(_: Client, message: Message):
    queue_size = video_queue.qsize()
    status = "🔄 جاري المعالجة" if is_processing else "⏸️ متوقف"
    await message.reply_text(
        f"📊 حالة قائمة المعالجة:\n📈 عدد الفيديوهات المتبقية: {queue_size}\n⚡ الحالة: {status}"
    )


@new_task
async def auto_command(_: Client, message: Message):
    global auto_process_enabled
    _, args = _extract_command_args(message)
    if not args:
        await message.reply_text(
            "❌ استخدم:\n`/{cmd} on` لتفعيل المعالجة التلقائية\n`/{cmd} off` لتعطيلها".format(
                cmd=_command_name("auto")
            )
        )
        return
    option = args[0].lower()
    if option == "on":
        auto_process_enabled = True
        await message.reply_text("✅ تم تفعيل المعالجة التلقائية للفيديوهات")
    elif option == "off":
        auto_process_enabled = False
        await message.reply_text("❌ تم تعطيل المعالجة التلقائية للفيديوهات")
    else:
        await message.reply_text("❌ خيار غير صحيح. استخدم `on` أو `off`")


@new_task
async def stop_command(_: Client, message: Message):
    queue_pause_event.clear()
    await message.reply_text("⏹️ تم إيقاف معالجة القائمة مؤقتاً.")


@new_task
async def resume_command(client: Client, message: Message):
    queue_pause_event.set()
    if not is_processing and not video_queue.empty():
        ensure_queue_processor(client)
        await message.reply_text("▶️ تم استئناف معالجة القائمة.")
    elif is_processing:
        await message.reply_text("ℹ️ المعالجة تعمل بالفعل.")
    else:
        await message.reply_text("ℹ️ لا توجد فيديوهات في القائمة.")


@new_task
async def clear_command(_: Client, message: Message):
    global is_processing
    cleared = 0
    while not video_queue.empty():
        try:
            video_queue.get_nowait()
            video_queue.task_done()
            cleared += 1
        except asyncio.QueueEmpty:
            break
    is_processing = False
    queue_pause_event.set()
    await message.reply_text(f"🗑️ تم مسح {cleared} فيديو من القائمة.")


@new_task
async def l_command(client: Client, message: Message):
    if _missing_config():
        await message.reply_text("⚠️ إعدادات ناقصة: تأكد من BUNNY_*.")
        return
    if len(message.command or []) < 2:
        await message.reply_text("❌ استخدم:\n`/{cmd} http://example.com/video.mp4`".format(cmd=_command_name("l")))
        return
    source_url = message.command[1].strip()
    note = await message.reply_text("☁️ بدء الاستيراد من الرابط ...")
    async with _import_semaphore:
        real_name = await asyncio.to_thread(_try_head_filename, source_url)
        if real_name:
            stem, ext = _split_name(real_name)
            base_name = f"{stem}{_ensure_mp4_ext(ext)}"
        else:
            base_name = f"{_derive_title_from_url(source_url)}.mp4"
        display_title = base_name
        try:
            existing_guid: Optional[str] = None
            if _db_available():
                await asyncio.to_thread(ensure_tables)
                existing_guid = await asyncio.to_thread(db_get_guid_by_url, source_url)
            if existing_guid:
                try:
                    info = await asyncio.to_thread(bunny_get_video, existing_guid)
                    current_title = (info.get("title") or "").strip()
                    if current_title != display_title:
                        await asyncio.to_thread(bunny_update_title, existing_guid, display_title)
                except Exception:
                    pass
                await safe_edit(note, "ℹ️ الفيديو موجود مسبقًا، سيتم الانتظار حتى اكتمال التجهيز ...")
                available = await wait_until_ready_full(
                    existing_guid,
                    status_msg=note,
                    base_name=base_name,
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
            if _db_available():
                await asyncio.to_thread(db_insert_import, source_url, video_guid, display_title)
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
            )
            if not available:
                await safe_edit(note, "⚠️ انتهت المهلة بدون جودات. جرّب لاحقًا.")
                return
            lines = build_bunny_links(video_guid, base_name, sorted(available))
            await safe_edit(note, "\n".join(lines))
        except requests.HTTPError as exc:
            try:
                await safe_edit(note, f"❌ خطأ HTTP: {exc} | {getattr(exc.response, 'text', '')}")
            except Exception:
                pass
        except Exception as exc:
            if "MESSAGE_NOT_MODIFIED" in str(exc):
                return
            try:
                await safe_edit(note, f"❌ حدث خطأ غير متوقع: {exc}")
            except Exception:
                pass


@new_task
async def ll_command(_: Client, message: Message):
    if len(message.command or []) < 2:
        await message.reply_text(
            "❌ استخدم:\n`/{cmd} https://iframe.mediadelivery.net/play/<LIB_ID>/<VIDEO_GUID>`".format(
                cmd=_command_name("ll")
            )
        )
        return
    url = message.command[1].strip()
    match = re.search(r"mediadelivery\.net/play/(\d+)/([a-f0-9\-]{36})", url, re.IGNORECASE)
    if not match:
        await message.reply_text(
            "⚠️ رابط غير صحيح. رجاءً أرسل لينك من شكل iframe.mediadelivery.net/play/<LIB>/<GUID>"
        )
        return
    video_guid = match.group(2)
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
    except requests.HTTPError as exc:
        await safe_edit(note, f"❌ خطأ HTTP: {exc} | {getattr(exc.response, 'text', '')}")
    except Exception as exc:
        if "MESSAGE_NOT_MODIFIED" in str(exc):
            return
        await safe_edit(note, f"❌ حدث خطأ غير متوقع: {exc}")


@new_task
async def ls_command(_: Client, message: Message):
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
        for entry in all_items:
            guid = entry.get("guid") or entry.get("videoGuid")
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
        blocks.sort(key=lambda item: item[0])
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = os.path.join(tmpdir, "bunny_links.txt")
            with open(out_path, "w", encoding="utf-8") as handle:
                for _, links in blocks:
                    for line in links:
                        handle.write(line + "\n")
                    handle.write("\n")
            await message.reply_document(out_path, caption="✅ تم إنشاء ملف الروابط لجميع الفيديوهات.")
        await note.delete()
    except requests.HTTPError as exc:
        await safe_edit(note, f"❌ خطأ HTTP: {exc} | {getattr(exc.response, 'text', '')}")
    except Exception as exc:
        if "MESSAGE_NOT_MODIFIED" in str(exc):
            return
        await safe_edit(note, f"❌ حدث خطأ غير متوقع: {exc}")


@new_task
async def dc_command(_: Client, message: Message):
    if _missing_config():
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
            for entry in items:
                guid = entry.get("guid") or entry.get("videoGuid")
                if not guid:
                    continue
                try:
                    await asyncio.to_thread(bunny_delete_video, guid)
                    total_deleted += 1
                except Exception as exc:
                    await message.reply_text(f"⚠️ فشل حذف {guid}: {exc}")
            total = data.get("totalItems") or 0
            if page * 100 >= total or not total:
                break
            page += 1
        await safe_edit(note, f"✅ تم حذف {total_deleted} فيديو من Bunny Stream.")
    except requests.HTTPError as exc:
        await safe_edit(note, f"❌ خطأ HTTP: {exc} | {getattr(exc.response, 'text', '')}")
    except Exception as exc:
        if "MESSAGE_NOT_MODIFIED" in str(exc):
            return
        await safe_edit(note, f"❌ حدث خطأ غير متوقع: {exc}")


@new_task
async def stats_command(_: Client, message: Message):
    if _missing_config():
        await message.reply_text("⚠️ إعدادات Bunny ناقصة.")
        return
    note = await message.reply_text("📊 جاري جمع الإحصائيات...")
    try:
        data = await asyncio.to_thread(bunny_list_videos, 1, 1)
        total_videos = data.get("totalItems", 0)
        resolution_count = {res: 0 for res in ALLOWED_RES}
        processed = 0
        sample_data = await asyncio.to_thread(bunny_list_videos, 1, min(20, max(1, total_videos)))
        sample_items = sample_data.get("items", [])
        for item in sample_items:
            guid = item.get("guid") or item.get("videoGuid")
            if not guid:
                continue
            try:
                info = await asyncio.to_thread(bunny_get_video, guid)
                available = _parse_available_resolutions(info).intersection(ALLOWED_RES)
                for res in available:
                    resolution_count.setdefault(res, 0)
                    resolution_count[res] += 1
                processed += 1
            except Exception:
                continue
        lines = ["📊 **إحصائيات مكتبة Bunny Stream**", f"🎬 **إجمالي الفيديوهات:** {total_videos}"]
        lines.append(f"📋 **تم فحص عينة:** {processed} فيديو")
        lines.append("📺 **الجودات المتاحة (في العينة):**")
        for res in sorted(ALLOWED_RES):
            count = resolution_count.get(res, 0)
            lines.append(f"• {res}p: {count} فيديو")
        lines.append("📊 **حالة القائمة الحالية:**")
        lines.append(f"• في الانتظار: {video_queue.qsize()} فيديو")
        lines.append(f"• الحالة: {'🔄 نشط' if is_processing else '⏸️ متوقف'}")
        await safe_edit(note, "\n".join(lines))
    except Exception as exc:
        await safe_edit(note, f"❌ خطأ في جمع الإحصائيات: {exc}")


@new_task
async def test_command(_: Client, message: Message):
    if _missing_config():
        await message.reply_text("⚠️ إعدادات Bunny ناقصة.")
        return
    note = await message.reply_text("🔄 اختبار الاتصال...")
    try:
        data = await asyncio.to_thread(bunny_list_videos, 1, 1)
        total = data.get("totalItems", 0)
        await safe_edit(note, f"✅ الاتصال سليم!\n📊 المكتبة تحتوي على {total} فيديو.")
    except requests.HTTPError as exc:
        await safe_edit(note, f"❌ خطأ في الاتصال: {exc}")
    except Exception as exc:
        await safe_edit(note, f"❌ خطأ غير متوقع: {exc}")


@new_task
async def auto_process_video(client: Client, message: Message):
    if not auto_process_enabled:
        return
    if not _is_video_message(message):
        return
    if not await can_process_message(client, message):
        return
    media = message.video or message.document
    if not media:
        return
    file_unique_id = media.file_unique_id
    if file_unique_id in processed_videos:
        return
    filename = getattr(media, "file_name", None) or f"video_{message.id}.mp4"
    ensure_queue_processor(client)
    queue_item = VideoQueueItem(
        message=message,
        file_name=filename,
        file_unique_id=file_unique_id,
        chat_id=message.chat.id,
    )
    await video_queue.put(queue_item)
    processed_videos.add(file_unique_id)
    total_in_queue = video_queue.qsize()
    status_lines = [
        "✅ تم إضافة الفيديو لقائمة المعالجة تلقائيًا",
        f"📊 إجمالي الفيديوهات في القائمة: {total_in_queue}",
    ]
    status_lines.append("⚡ المعالجة جارية حالياً..." if is_processing else "🚀 سيبدأ المعالجة خلال ثوانٍ...")
    try:
        await message.reply_text("\n".join(status_lines))
    except Exception:
        pass


def _register_alias_functions() -> None:
    for base, trigger in COMMAND_TRIGGER_MAP.items():
        if not trigger or trigger == base:
            continue
        if not trigger.isidentifier():
            continue
        base_attr = f"{base}_command"
        alias_attr = f"{trigger}_command"
        base_func = globals().get(base_attr)
        if base_func is None or alias_attr in globals():
            continue
        globals()[alias_attr] = base_func


_register_alias_functions()


class BunnyStreamPlugin(PluginBase):
    PLUGIN_INFO = PluginInfo(
        name="bunny_stream",
        version="1.0.0",
        author="OpenAI Assistant",
        description="إدارة رفع واستيراد الفيديوهات تلقائيًا إلى Bunny Stream",
        enabled=True,
        handlers=[],
        commands=_registered_command_names(),
        dependencies=[],
    )

    async def on_load(self) -> bool:
        plugin_manager = get_plugin_manager()
        if plugin_manager.bot is not None:
            ensure_queue_processor(plugin_manager.bot)
        LOGGER.info("Bunny Stream plugin loaded")
        return True

    async def on_unload(self) -> bool:
        LOGGER.info("Bunny Stream plugin unloaded")
        return True

    async def on_enable(self) -> bool:
        LOGGER.info("Bunny Stream plugin enabled")
        return True

    async def on_disable(self) -> bool:
        LOGGER.info("Bunny Stream plugin disabled")
        return True


# Register auto-processing handler
AUTO_HANDLER = filters.group & (filters.video | filters.document) & CustomFilters.authorized
BunnyStreamPlugin.PLUGIN_INFO.handlers.append(MessageHandler(auto_process_video, AUTO_HANDLER))


__all__ = ["BunnyStreamPlugin"]
