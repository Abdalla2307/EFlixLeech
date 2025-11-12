set -e

cd "$(dirname "$0")"

export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

python3 update.py && python3 -m bot
