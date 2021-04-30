#!/bin/bash
VENV='TYPE_YOUR_VENV_NAME_HERE'  # e.g.: py3.6.9
# get directory of this script (https://stackoverflow.com/a/246128):
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
# activate virtualenv
source "$SCRIPT_DIR/.env/$VENV/bin/activate"
# start download
python -c "$SCRIPT_DIR/s2s_config/download.private.yaml"