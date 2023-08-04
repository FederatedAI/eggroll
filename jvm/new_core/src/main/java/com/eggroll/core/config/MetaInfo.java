/*
 * Copyright 2019 The FATE Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.eggroll.core.config;

import com.eggroll.core.exceptions.ConfigErrorException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetaInfo {

    static Logger logger = LoggerFactory.getLogger(MetaInfo.class);

    @Config(confKey = "eggroll.zookeeper.register.host")
    public static String ZOOKEEPER_HOST = "";
    @Config(confKey = "eggroll.zookeeper.register.version")
    public static String ZOOKEEPER_VERSION = "";
    @Config(confKey = "eggroll.zookeeper.register.port", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer ZOOKEEPER_PORT = 0  ;
    @Config(confKey = "eggroll.zookeeper.register.enable", pattern = Dict.BOOLEAN_PATTERN)
    public static Boolean ZOOKEEPER_ENABLED;

    @Config(confKey = "grpc.server.max.concurrent.call.per.connection", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static int PROPERTY_GRPC_SERVER_MAX_CONCURRENT_CALL_PER_CONNECTION = 1000;

    @Config(confKey = "eggroll.logs.dir")
    public static String EGGROLL_LOGS_DIR = "";

    @Config(confKey = "eggroll.data.dir")
    public static String EGGROLL_DATA_DIR = "";
    @Config(confKey = "eggroll.static.conf.path")
    public static String STATIC_CONF_PATH = "";
    @Config(confKey = "eggroll.bootstrap.root.script")
    public static String BOOTSTRAP_ROOT_SCRIPT = "";
    @Config(confKey = "eggroll.bootstrap.shell")
    public static String BOOTSTRAP_SHELL = "";
    @Config(confKey = "eggroll.bootstrap.shell.args")
    public static String BOOTSTRAP_SHELL_ARGS = "";
    @Config(confKey = "eggroll.core.grpc.transfer.server.host")
    public static String CONFKEY_CORE_GRPC_TRANSFER_SERVER_HOST = "0.0.0.0";
    @Config(confKey = "eggroll.core.grpc.transfer.server.port")
    public static String CONFKEY_CORE_GRPC_TRANSFER_SERVER_PORT = "0";


    @Config(confKey = "eggroll.core.grpc.channel.cache.expire.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_CACHE_EXPIRE_SEC = 86400;
    @Config(confKey = "eggroll.core.grpc.channel.cache.size", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_CACHE_SIZE = 5000;
    @Config(confKey = "eggroll.core.grpc.channel.executor.pool.size", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_EXECUTOR_POOL_SIZE = 128;
    @Config(confKey = "eggroll.core.grpc.channel.flow.control.window", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_FLOW_CONTROL_WINDOW = 128 << 20;
    @Config(confKey = "eggroll.core.grpc.channel.idle.timeout.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_IDLE_TIMEOUT_SEC = 86400;
    @Config(confKey = "eggroll.core.grpc.channel.keepalive.time.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIME_SEC = 7200;
    @Config(confKey = "eggroll.core.grpc.channel.keepalive.timeout.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIMEOUT_SEC = 3600;
    @Config(confKey = "eggroll.core.grpc.channel.max.inbound.metadata.size", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_MAX_INBOUND_METADATA_SIZE = 128 << 20;
    @Config(confKey = "eggroll.core.grpc.channel.max.inbound.message.size", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_MAX_INBOUND_MESSAGE_SIZE = (2 << 30) - 1;
    @Config(confKey = "eggroll.grpc.channel.max.retry.attempts", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_MAX_RETRY_ATTEMPTS = 5;

    @Config(confKey = "eggroll.core.grpc.channel.per.rpc.buffer.limit", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_PER_RPC_BUFFER_LIMIT = 64 << 20;
    @Config(confKey = "eggroll.core.grpc.channel.retry.buffer.size", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_RETRY_BUFFER_SIZE = 16 << 20;
    @Config(confKey = "eggroll.core.grpc.channel.ssl.session.cache.size", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_SSL_SESSION_CACHE_SIZE = 65536;
    @Config(confKey = "eggroll.core.grpc.channel.ssl.session.timeout.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_SSL_SESSION_TIMEOUT_SEC = 7200;
    @Config(confKey = "eggroll.core.grpc.channel.termination.await.timeout.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_TERMINATION_AWAIT_TIMEOUT_SEC = 20;


    @Config(confKey = "eggroll.core.grpc.channel.keepalive.without.calls.enabled", pattern = Dict.BOOLEAN_PATTERN)
    public static Boolean CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED = false;
    @Config(confKey = "eggroll.core.grpc.server.channel.keepalive.without.calls.enabled", pattern = Dict.BOOLEAN_PATTERN)
    public static Boolean CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED = false;


    @Config(confKey = "eggroll.core.grpc.server.channel.flow.control.window", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_FLOW_CONTROL_WINDOW = 128 << 20;
    @Config(confKey = "eggroll.core.grpc.server.channel.keepalive.time.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIME_SEC = 7200;
    @Config(confKey = "eggroll.core.grpc.server.channel.keepalive.timeout.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIMEOUT_SEC = 3600;
    @Config(confKey = "eggroll.core.grpc.server.channel.max.concurrent.call.per.connection", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONCURRENT_CALL_PER_CONNECTION = 1000;
    @Config(confKey = "eggroll.core.grpc.server.channel.max.connection.age.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_AGE_SEC = 86400;
    @Config(confKey = "eggroll.core.grpc.server.channel.max.connection.age.grace.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_AGE_GRACE_SEC = 86400;
    @Config(confKey = "eggroll.core.grpc.server.channel.max.connection.idle.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_IDLE_SEC = 86400;
    @Config(confKey = "eggroll.core.grpc.server.channel.max.inbound.message.size", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_MESSAGE_SIZE = (2 << 30) - 1;
    @Config(confKey = "eggroll.core.grpc.server.channel.max.inbound.metadata.size", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_METADATA_SIZE = 128 << 20;
    @Config(confKey = "eggroll.core.grpc.server.channel.permit.keepalive.time.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_PERMIT_KEEPALIVE_TIME_SEC = 120;
    @Config(confKey = "eggroll.core.grpc.server.channel.ssl.session.cache.size", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_SSL_SESSION_CACHE_SIZE = 65536;
    @Config(confKey = "eggroll.core.grpc.server.channel.ssl.session.timeout.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_SSL_SESSION_TIMEOUT_SEC = 86400;


    @Config(confKey = "eggroll.core.retry.default.attempt.timeout.ms", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_RETRY_DEFAULT_ATTEMPT_TIMEOUT_MS = 30000;
    @Config(confKey = "eggroll.core.retry.default.max.attempts", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_RETRY_DEFAULT_MAX_ATTEMPTS = 3;
    @Config(confKey = "eggroll.core.retry.default.wait.time.ms", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_RETRY_DEFAULT_WAIT_TIME_MS = 1000;

    @Config(confKey = "eggroll.core.security.ca.crt.path")
    public static String CONFKEY_CORE_SECURITY_CA_CRT_PATH;

    @Config(confKey = "eggroll.core.security.crt.path")
    public static String CONFKEY_CORE_SECURITY_KEY_CRT_PATH;
    @Config(confKey = "eggroll.core.security.key.path")
    public static String CONFKEY_CORE_SECURITY_KEY_PATH;


    @Config(confKey = "eggroll.core.security.client.ca.crt.path")
    public static String CONFKEY_CORE_SECURITY_CLIENT_CA_CRT_PATH;
    @Config(confKey = "eggroll.core.security.client.crt.path")
    public static String CONFKEY_CORE_SECURITY_CLIENT_KEY_CRT_PATH;
    @Config(confKey = "eggroll.core.security.client.key.path")
    public static String CONFKEY_CORE_SECURITY_CLIENT_KEY_PATH;

    @Config(confKey = "eggroll.core.security.secure.cluster.enabled", pattern = Dict.BOOLEAN_PATTERN)
    public static Boolean CONFKEY_CORE_SECURITY_SECURE_CLUSTER_ENABLED = false;
    @Config(confKey = "eggroll.core.security.secure.client.auth.enabled", pattern = Dict.BOOLEAN_PATTERN)
    public static Boolean CONFKEY_CORE_SECURITY_CLIENT_AUTH_ENABLED = false;


    @Config(confKey = "eggroll.core.command.default.serdes.type")
    public static String CONFKEY_CORE_COMMAND_DEFAULT_SERDES_TYPE;
    @Config(confKey = "eggroll.core.log.dir")
    public static String CONFKEY_CORE_LOG_DIR;

    @Config(confKey = "eggroll.core.stats.direct.memory.metrics", pattern = Dict.BOOLEAN_PATTERN)
    public static Boolean EGGROLL_CORE_STATS_DIRECT_MEMORY_METRICS = false;
    @Config(confKey = "eggroll.core.stats.direct.memory.metrics.interval.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer EGGROLL_CORE_STATS_DIRECT_MEMORY_METRICS_INTERVAL_SEC = 60;

    //ClusterManagerConfKeys
    @Config(confKey = "eggroll.resourcemanager.clustermanager.jdbc.driver.class.name")
    public static String CONFKEY_CLUSTER_MANAGER_JDBC_DRIVER_CLASS_NAME;
    @Config(confKey = "eggroll.resourcemanager.clustermanager.jdbc.url")
    public static String CONFKEY_CLUSTER_MANAGER_JDBC_URL;
    @Config(confKey = "eggroll.resourcemanager.clustermanager.jdbc.username")
    public static String CONFKEY_CLUSTER_MANAGER_JDBC_USERNAME;
    @Config(confKey = "eggroll.resourcemanager.clustermanager.jdbc.password")
    public static String CONFKEY_CLUSTER_MANAGER_JDBC_PASSWORD;
    @Config(confKey = "eggroll.resourcemanager.clustermanager.host")
    public static String CONFKEY_CLUSTER_MANAGER_HOST;
    @Config(confKey = "eggroll.resourcemanager.clustermanager.jdbc.password.decryptor")
    public static String EGGROLL_RESOURCEMANAGER_CLUSTERMANAGER_JDBC_PASSWORD_DECRYPTOR;
    @Config(confKey = "eggroll.resourcemanager.clustermanager.jdbc.password.decryptor")
    public static String EGGROLL_RESOURCEMANAGER_CLUSTERMANAGER_JDBC_PASSWORD_DECRYPTOR_ARGS;
    @Config(confKey = "eggroll.resourcemanager.clustermanager.jdbc.password.decryptor.args.spliter")
    public static String EGGROLL_RESOURCEMANAGER_CLUSTERMANAGER_JDBC_PASSWORD_DECRYPTOR_ARGS_SPLITER = ",";

    @Config(confKey = "eggroll.resourcemanager.clustermanager.datasource.db.max.idle", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_IDLE;
    @Config(confKey = "eggroll.resourcemanager.clustermanager.datasource.db.max.total", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_TOTAL;
    @Config(confKey = "eggroll.resourcemanager.clustermanager.datasource.db.max.wait.ms", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_WAIT_MS;
    @Config(confKey = "eggroll.resourcemanager.clustermanager.datasource.db.time.between.eviction.runs.ms", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_TIME_BETWEEN_EVICTION_RUNS_MS;
    @Config(confKey = "eggroll.resourcemanager.clustermanager.datasource.db.min.evictable.idle.time.ms", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MIN_EVICTABLE_IDLE_TIME_MS;
    @Config(confKey = "eggroll.resourcemanager.clustermanager.datasource.db.default.auto.commit", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_DEFAULT_AUTO_COMMIT;
    @Config(confKey = "eggroll.resourcemanager.clustermanager.port")
    public static Integer CONFKEY_CLUSTER_MANAGER_PORT = 4670;
    @Config(confKey = "eggroll.resourcemanager.nodemanager.port")
    public static Integer CONFKEY_NODE_MANAGER_PORT = 4671;
    @Config(confKey = "eggroll.resourcemanager.clustermanager.node.heartbeat.expire.count", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CLUSTER_MANAGER_NODE_HEARTBEAT_EXPIRED_COUNT = 2;
    @Config(confKey = "eggroll.resourcemanager.schedule.minimum-allocation-vcores", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer EGGROLL_RESOURCEMANAGER_SCHEDULE_MINIMUM_ALLOCATION_VCORES = 1;

    //NodeManagerConfKeys
    @Config(confKey = "eggroll.resourcemanager.nodemanager.host")
    public static String CONFKEY_NODE_MANAGER_HOST;
    @Config(confKey = "eggroll.resourcemanager.nodemanager.containers.data.dir")
    public static String CONFKEY_NODE_MANAGER_CONTAINERS_DATA_DIR;

    @Config(confKey = "eggroll.resourcemanager.nodemanager.id", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_NODE_MANAGER_ID;
    @Config(confKey = "eggroll.resourcemanager.nodemanager.heartbeat.interval", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL = 10000;
    @Config(confKey = "eggroll.resourcemanager.nodemanager.cpu.vcores", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_NODE_MANAGER_CPU_VCORES = 16;
    @Config(confKey = "eggroll.resourcemanager.nodemanager.gpu.vcores", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_NODE_MANAGER_GPU_VCORES = 16;

    //ResourceManagerConfKeys
    @Config(confKey = "eggroll.resourcemanager.server.node.id")
    public static String SERVER_NODE_ID;

    //SessionConfKeys
    @Config(confKey = "eggroll.session.name")
    public static String CONFKEY_SESSION_NAME;
    @Config(confKey = "python.path")
    public static String EGGROLL_SESSION_PYTHON_PATH;
    @Config(confKey = "python.venv")
    public static String EGGROLL_SESSION_PYTHON_VENV;
    @Config(confKey = "eggroll.session.use.resource.dispatch")
    public static Boolean EGGROLL_SESSION_USE_RESOURCE_DISPATCH = false;
    @Config(confKey = "eggroll.session.processors.per.node")
    public static Integer CONFKEY_SESSION_PROCESSORS_PER_NODE = 1;


    @Config(confKey = "grpc.client.max.inbound.message.size", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static int PROPERTY_GRPC_CLIENT_MAX_INBOUND_MESSAGE_SIZE = (2 << 30) - 1;
    @Config(confKey = "grpc.client.flow.control.window", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static int PROPERTY_GRPC_CLIENT_FLOW_CONTROL_WINDOW = 128 << 20;
    @Config(confKey = "grpc.client.keepalive.time", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static int PROPERTY_GRPC_CLIENT_KEEPALIVE_TIME_SEC = 7200;
    @Config(confKey = "grpc.client.keepalive.timeout", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static int PROPERTY_GRPC_CLIENT_KEEPALIVE_TIMEOUT_SEC = 3600;
    @Config(confKey = "grpc.client.keepalive.without.calls.enabled", pattern = Dict.BOOLEAN_PATTERN)
    public static boolean PROPERTY_GRPC_CLIENT_KEEPALIVE_WITHOUT_CALLS_ENABLED = true;
    @Config(confKey = "grpc.client.max.connection.idle", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static int PROPERTY_GRPC_CLIENT_MAX_CONNECTION_IDLE_SEC = 86400;
    @Config(confKey = "grpc.client.per.rpc.buffer.limit", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static int PROPERTY_GRPC_CLIENT_PER_RPC_BUFFER_LIMIT = (2 << 30) - 1;

    @Config(confKey = "grpc.client.retry.buffer.size", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static int PROPERTY_GRPC_CLIENT_RETRY_BUFFER_SIZE = 86400;


    @Config(confKey = "eggroll.session.context.rollpair.count", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_SESSION_CONTEXT_ROLLPAIR_COUNT;
    @Config(confKey = "eggroll.session.id", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_SESSION_ID;

    @Config(confKey = "eggroll.session.start.timeout.ms", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer EGGROLL_SESSION_START_TIMEOUT_MS = 20000;


    @Config(confKey = "eggroll.session.status.new.timeout.ms", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer EGGROLL_SESSION_STATUS_NEW_TIMEOUT_MS = 8 * 3600 * 1000 + 20000;
    @Config(confKey = "eggroll.session.stop.timeout.ms", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer EGGROLL_SESSION_STOP_TIMEOUT_MS = 20000;
    @Config(confKey = "eggroll.session.max.live.ms", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer EGGROLL_SESSION_MAX_LIVE_MS = 48 * 3600 * 1000;
    @Config(confKey = "eggroll.session.status.check.interval.ms", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer EGGROLL_SESSION_STATUS_CHECK_INTERVAL_MS = 5000;

    @Config(confKey = "eggroll.session.resource.dispatch.interval", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer EGGROLL_RESOURCE_DISPATCH_INTERVAL = 3000;
    @Config(confKey = "eggroll.resource.lock.expire.interval", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer EGGROLL_RESOURCE_LOCK_EXPIRE_INTERVAL = 3600000;

    public static boolean checkPattern(String pattern, String value) {
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(value);
        if (m.find()) {
            return true;
        } else {
            return false;
        }
    }

    public static void init(Properties environment) {
        Field[] fields = MetaInfo.class.getFields();
        Arrays.stream(fields).forEach(field -> {
            try {
                Config config = field.getDeclaredAnnotation(Config.class);
                if (config != null) {
                    Class clazz = field.getType();
                    String confKey = config.confKey();
                    Object value = environment.get(confKey);
                    if (value != null) {
                        String pattern = config.pattern();
                        if (StringUtils.isNotEmpty(pattern) && !checkPattern(pattern, value.toString())) {
                            logger.error("conf {} has wrong value {},please check config file", confKey, value);
                            throw new ConfigErrorException("conf " + confKey + " has wrong value : " + value);
                        }
                        if (clazz == Integer.class) {
                            field.set(null, Integer.parseInt(value.toString()));
                        } else if (clazz == Long.class) {
                            field.set(null, Long.parseLong(value.toString()));
                        } else if (clazz == String.class) {
                            field.set(null, value.toString());

                        } else if (clazz == Boolean.class) {
                            field.set(null, Boolean.valueOf(value.toString()));
                        } else if (clazz.isAssignableFrom(Set.class)) {
                            Set set = new HashSet();
                            set.addAll(Lists.newArrayList(value.toString().split(",")));
                            field.set(null, set);
                        }
                    }
                    if (StringUtils.isNotEmpty(confKey)) {
                        logger.info("{}={} ", confKey, field.get(null));
                    }
                }
            } catch (Exception e) {
                //   e.printStackTrace();
                logger.error("parse config error", e);
                throw new ConfigErrorException("parse config error: " + e.getMessage());
            }
        });
    }


    public static Map toMap() {
        Map result = Maps.newHashMap();
        Field[] fields = MetaInfo.class.getFields();

        for (Field field : fields) {
            try {
                if (field.get(MetaInfo.class) != null) {
                    String key = Dict.class.getField(field.getName()) != null ? String.valueOf(Dict.class.getField(field.getName()).get(Dict.class)) : field.getName();
                    result.put(key, field.get(MetaInfo.class));
                }
            } catch (IllegalAccessException | NoSuchFieldException e) {

            }
        }
        return result;
    }

}
