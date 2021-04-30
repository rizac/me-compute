#!/bin/bash
# get directory of this script (https://stackoverflow.com/a/246128):
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
# activate virrualenv
source "$SCRIPT_DIR/stream2segment/.env/py3.6.9/bin/activate"
# start download
s2s download -c "$SCRIPT_DIR/s2s_config/download.private.yaml"