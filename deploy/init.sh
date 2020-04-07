export EGGROLL_HOME= 
export PYTHONPATH=$EGGROLL_HOME/python 
export EGGROLL_LOG_LEVEL=DEBUG
export EGGROLL_RESOURCE_MANAGER_AUTO_BOOTSTRAP=0
export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=
source $EGGROLL_HOME/venv/bin/activate

echo "===init start==="
echo "EGGROLL_HOME:$EGGROLL_HOME"
echo "PYTHONPATH:$PYTHONPATH"
echo "pwd:`pwd`"
echo "===init end===" 
