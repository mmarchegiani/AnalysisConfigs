export PYTHONPATH="${PWD}:${PYTHONPATH}"
export LAW_CONFIG_FILE="${PWD}/law.cfg"

source "$( law completion )" ""
