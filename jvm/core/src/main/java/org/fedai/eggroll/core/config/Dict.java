package org.fedai.eggroll.core.config;

public class Dict {


    public static final String SERDES_TYPE_PICKLE = "PICKLE";
    public static final String SERDES_TYPE_PROTOBUF = "PROTOBUF";
    public static final String SERDES_TYPE_CLOUD_EMPTY = "EMPTY";


    public static final String KEY_SERVER_NODE_ID = "serverNodeId";
    public static final String KEY_CLUSTER_ID = "clusterId";
    public static final String KEY_UPDATE_TIME = "updateTime";
    public static final String KEY_NODE_IP = "nodeIp";
    public static final String KEY_NODE_PORT = "nodePort";
    public static final String STATUS = "status";
    public static final String KEY_PROCESSOR_TYPE = "ProcessorType";

    // checkResourceEnough
    public static final String CHECK_RESOURCE_ENOUGH_CHECK_TYPE_NODE = "nodeCheck";
    public static final String CHECK_RESOURCE_ENOUGH_CHECK_TYPE_CLUSTER = "clusterCheck";
    public static final String RESOURCE_TYPE_GPU = "gpu";
    public static final String RESOURCE_TYPE_CPU = "cpu";
    public static final String RESOURCE_TYPE_MEMORY = "memory";

    public static final String SERVER_NODES = "SERVER_NODES";
    public static final String SESSION_IN_DB = "SESSION_IN_DB";
    public static final String PROCESSOR_IN_DB = "PROCESSOR_IN_DB";
    public static final String OPEN_ASYN_POST_HANDLE = "OPEN_ASYN_POST_HANDLE";
    public static final String IS_BREAK = "IS_BREAK";
    public static final String SERVER_NODE_ID = "eggroll.resourcemanager.server.node.id";
    public static final String POSITIVE_INTEGER_PATTERN = "^[1-9]\\d*$";
    public static final String BOOLEAN_PATTERN = "^(true)|(false)$";
    public static String CONFKEY_CORE_GRPC_TRANSFER_SECURE_SERVER_ENABLED = "eggroll.core.grpc.transfer.secure.server.enabled";

    public static final String BEFORE_STATUS = "beforeStatus";
    public static final String STATUS_REASON = "statusReason";

    public static final String ROLLFRAME_FILE = "ROLL_FRAME_FILE";
    public static final String ROLLPAIR_IN_MEMORY = "IN_MEMORY";
    public static final String ROLLPAIR_LEVELDB = "LEVELDB";
    public static final String ROLLPAIR_LMDB = "LMDB";

    public static final String BYTESTRING_HASH = "BYTESTRING_HASH";

    public static final String PICKLE = "PICKLE";
    public static final String PROTOBUF = "PROTOBUF";
    public static final String CLOUD_PICKLE = "CLOUD_PICKLE";
    public static final String EMPTY = "EMPTY";

    public static final String NORMAL = "NORMAL";
    public static final String DELETED = "DELETED";

    public static final String PRIMARY = "PRIMARY";
    public static final String BACKUP = "BACKUP";
    public static final String MISSING = "MISSING";

    public static final String TRANSFER_END = "__transfer_end";


    public static final String ROLL_PAIR = "roll_pair";
    public static final String ROLL_PAILLIER_TENSOR = "roll_paillier_tensor";
    public static final String EGG_FRAME = "egg_frame";
    public static final String ROLL_FRAME = "roll_frame";
    public static final String ROLL_PAIR_MASTER = "roll_pair_master";
    public static final String EGG_PAIR = "egg_pair";

    public static final String CLUSTER_MANAGER = "CLUSTER_MANAGER";
    public static final String NODE_MANAGER = "NODE_MANAGER";


    public static final String PHYSICAL_MEMORY = "PHYSICAL_MEMORY";
    public static final String VCPU_CORE = "VCPU_CORE";
    public static final String VGPU_CORE = "VGPU_CORE";

    public static final String PRE_ALLOCATED = "pre_allocated";
    public static final String ALLOCATED = "allocated";
    public static final String ALLOCATE_FAILED = "allocate_failed";
    public static final String AVAILABLE = "available";
    public static final String RETURN = "return";

    public static final String CHECK = "CHECK";
    public static final String ALLOCATE = "ALLOCATE";
    public static final String FREE = "FREE";

    public static final String SUCCESS = "SUCCESS";
    public static final String FAILED = "FAILED";

    public static final String PROCESSOR_LOSS = "PROCESSOR_LOSS";

    public static final String REMAIN_MOST_FIRST = "remain_most_first";
    public static final String RANDOM = "random";
    public static final String FIX = "fix";
    public static final String SINGLE_NODE_FIRST = "single_node_first";

    public static final String RESOURCE_RETURN = "resource_return";
    public static final String RESOURCE_ALLOCATED = "resource_allocated";

    public static final String IGNORE = "ignore";
    public static final String WAITING = "waiting";
    public static final String THROW_ERROR = "throw_error";

    public static final String HEALTHY = "HEALTHY";
    public static final String INIT = "INIT";
    public static final String LOSS = "LOSS";

    public static final String RUNNING = "RUNNING";
    public static final String STOPPED = "STOPPED";
    public static final String ERROR = "ERROR";
    public static final String FINISHED = "FINISHED";

    public static final String NEW = "NEW";
    public static final String NEW_TIMEOUT = "NEW_TIMEOUT";
    public static final String ACTIVE = "ACTIVE";
    public static final String CLOSED = "CLOSED";
    public static final String KILLED = "KILLED";


    public static final String ROUND_ROBIN = "ROUND_ROBIN";
    public static final String CONFKEY_SESSION_PROCESSORS_PER_NODE = "eggroll.session.processors.per.node";

    public static final String NODE_CMD_START = "start";
    public static final String NODE_CMD_STOP = "stop";
    public static final String NODE_CMD_KILL = "kill";


    // CoreConfKeys
    public static final String EGGROLL_LOGS_DIR = "eggroll.logs.dir";
    public static final String EGGROLL_DATA_DIR = "eggroll.data.dir";
    public static final String STATIC_CONF_PATH = "eggroll.static.conf.path";
    public static final String BOOTSTRAP_ROOT_SCRIPT = "eggroll.bootstrap.root.script";
    public static final String BOOTSTRAP_SHELL = "eggroll.bootstrap.shell";
    public static final String BOOTSTRAP_SHELL_ARGS = "eggroll.bootstrap.shell.args";
    public static final String EGGROLL_RESOURCEMANAGER_BOOTSTRAP = "eggroll.resourcemanager.bootstrap";


    // NodeManagerConfKeys
    public static final String CONFKEY_NODE_MANAGER_HOST = "eggroll.resourcemanager.nodemanager.host";
    public static final String CONFKEY_NODE_MANAGER_PORT = "eggroll.resourcemanager.nodemanager.port";
    public static final String DEEPSPEED_PYTHON_EXEC = "eggroll.container.deepspeed.python.exec";
    // ClusterManagerConfKeys
    public static final String CONFKEY_CLUSTER_MANAGER_HOST = "eggroll.resourcemanager.clustermanager.host";
    public static final String CONFKEY_CLUSTER_MANAGER_PORT = "eggroll.resourcemanager.clustermanager.port";
    public static final String DEEPSPEED_SCRIPT_PATH = "eggroll.container.deepspeed.script.path";


    // SessionConfKeys
    public static final String CONFKEY_SESSION_ID = "eggroll.session.id";
    public static final String CONFKEY_SESSION_NAME = "eggroll.session.name";
    public static final String EGGROLL_SESSION_PYTHON_PATH = "python.path";
    public static final String EGGROLL_SESSION_PYTHON_VENV = "python.venv";

    public static final String MODELS = "models";
    public static final String LOGS = "logs";

    public static final String RESULT = "result";
    public static final String ZIP = "zip";
    public static final String IP = "ip";
    public static final String PORT = "port";
    public static final String DS_DOWNLOAD = "DS-DOWNLOAD";


    public static final String SCHEDULE_KEY = "schedule_key";
    public static final String HEART_BEAT = "HEART BEAT";


}
