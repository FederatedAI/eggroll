package com.eggroll.core.config;

public class Dict {

    public static final String POSITIVE_INTEGER_PATTERN = "^[1-9]\\d*$";
    public static final String BOOLEAN_PATTERN = "^(true)|(false)$";
    public static String CONFKEY_CORE_GRPC_TRANSFER_SECURE_SERVER_ENABLED = "eggroll.core.grpc.transfer.secure.server.enabled";


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

    public static void main(String[] args) {
        System.out.println("tttttttttttttttttttt");
    }

}
