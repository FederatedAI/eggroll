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

package com.webank.eggroll.core.config;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.webank.eggroll.core.constant.Dict;
import com.webank.eggroll.core.exceptions.ConfigErrorException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetaInfo {

    static Logger logger = LoggerFactory.getLogger(MetaInfo.class);


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
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_CACHE_EXPIRE_SEC =86400  ;
    @Config(confKey = "eggroll.core.grpc.channel.cache.size", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_CACHE_SIZE = 5000;
    @Config(confKey = "eggroll.core.grpc.channel.executor.pool.size", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_EXECUTOR_POOL_SIZE = 128;
    @Config(confKey = "eggroll.core.grpc.channel.flow.control.window", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_FLOW_CONTROL_WINDOW = 128 << 20;
    @Config(confKey = "eggroll.core.grpc.channel.idle.timeout.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_IDLE_TIMEOUT_SEC =86400;
    @Config(confKey = "eggroll.core.grpc.channel.keepalive.time.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIME_SEC = 7200;
    @Config(confKey = "eggroll.core.grpc.channel.keepalive.timeout.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIMEOUT_SEC =3600;
    @Config(confKey = "eggroll.core.grpc.channel.max.inbound.metadata.size", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_MAX_INBOUND_METADATA_SIZE =128 << 20;
    @Config(confKey = "eggroll.core.grpc.channel.max.inbound.message.size", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_MAX_INBOUND_MESSAGE_SIZE =(2 << 30) - 1;
    @Config(confKey = "eggroll.grpc.channel.max.retry.attempts", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_MAX_RETRY_ATTEMPTS  = 5;

    @Config(confKey = "eggroll.core.grpc.channel.per.rpc.buffer.limit", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_PER_RPC_BUFFER_LIMIT =64 << 20;
    @Config(confKey = "eggroll.core.grpc.channel.retry.buffer.size", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_RETRY_BUFFER_SIZE = 16 << 20;
    @Config(confKey = "eggroll.core.grpc.channel.ssl.session.cache.size", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_SSL_SESSION_CACHE_SIZE =65536;
    @Config(confKey = "eggroll.core.grpc.channel.ssl.session.timeout.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_SSL_SESSION_TIMEOUT_SEC =7200;
    @Config(confKey = "eggroll.core.grpc.channel.termination.await.timeout.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_CHANNEL_TERMINATION_AWAIT_TIMEOUT_SEC =20;



    @Config(confKey = "eggroll.core.grpc.channel.keepalive.without.calls.enabled", pattern = Dict.BOOLEAN_PATTERN)
    public static Boolean CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED = false;
    @Config(confKey = "eggroll.core.grpc.server.channel.keepalive.without.calls.enabled", pattern = Dict.BOOLEAN_PATTERN)
    public static Boolean CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED =false;


    @Config(confKey = "eggroll.core.grpc.server.channel.flow.control.window", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_FLOW_CONTROL_WINDOW =128 << 20;
    @Config(confKey = "eggroll.core.grpc.server.channel.keepalive.time.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIME_SEC =7200;
    @Config(confKey = "eggroll.core.grpc.server.channel.keepalive.timeout.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIMEOUT_SEC =3600;
    @Config(confKey = "eggroll.core.grpc.server.channel.max.concurrent.call.per.connection", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONCURRENT_CALL_PER_CONNECTION =1000;
    @Config(confKey = "eggroll.core.grpc.server.channel.max.connection.age.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_AGE_SEC =86400;
    @Config(confKey = "eggroll.core.grpc.server.channel.max.connection.age.grace.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_AGE_GRACE_SEC =86400;
    @Config(confKey = "eggroll.core.grpc.server.channel.max.connection.idle.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_IDLE_SEC =86400;
    @Config(confKey = "eggroll.core.grpc.server.channel.max.inbound.message.size", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_MESSAGE_SIZE =(2 << 30) - 1;
    @Config(confKey = "eggroll.core.grpc.server.channel.max.inbound.metadata.size", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_METADATA_SIZE =128 << 20;
    @Config(confKey = "eggroll.core.grpc.server.channel.permit.keepalive.time.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_PERMIT_KEEPALIVE_TIME_SEC =120;
    @Config(confKey = "eggroll.core.grpc.server.channel.ssl.session.cache.size", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_SSL_SESSION_CACHE_SIZE =65536;
    @Config(confKey = "eggroll.core.grpc.server.channel.ssl.session.timeout.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_GRPC_SERVER_CHANNEL_SSL_SESSION_TIMEOUT_SEC =86400;


    @Config(confKey = "eggroll.core.retry.default.attempt.timeout.ms", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_RETRY_DEFAULT_ATTEMPT_TIMEOUT_MS =30000;
    @Config(confKey = "eggroll.core.retry.default.max.attempts", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_RETRY_DEFAULT_MAX_ATTEMPTS =3;
    @Config(confKey = "eggroll.core.retry.default.wait.time.ms", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer CONFKEY_CORE_RETRY_DEFAULT_WAIT_TIME_MS =1000;

    @Config(confKey = "eggroll.core.security.ca.crt.path")
    public static String CONFKEY_CORE_SECURITY_CA_CRT_PATH ;

    @Config(confKey = "eggroll.core.security.crt.path")
    public static String CONFKEY_CORE_SECURITY_KEY_CRT_PATH ;
    @Config(confKey = "eggroll.core.security.key.path")
    public static String CONFKEY_CORE_SECURITY_KEY_PATH ;


    @Config(confKey = "eggroll.core.security.client.ca.crt.path")
    public static String CONFKEY_CORE_SECURITY_CLIENT_CA_CRT_PATH ;
    @Config(confKey = "eggroll.core.security.client.crt.path")
    public static String CONFKEY_CORE_SECURITY_CLIENT_KEY_CRT_PATH ;
    @Config(confKey = "eggroll.core.security.client.key.path")
    public static String CONFKEY_CORE_SECURITY_CLIENT_KEY_PATH ;

    @Config(confKey = "eggroll.core.security.secure.cluster.enabled", pattern = Dict.BOOLEAN_PATTERN)
    public static Boolean CONFKEY_CORE_SECURITY_SECURE_CLUSTER_ENABLED =false;
    @Config(confKey = "eggroll.core.security.secure.client.auth.enabled", pattern = Dict.BOOLEAN_PATTERN)
    public static Boolean CONFKEY_CORE_SECURITY_CLIENT_AUTH_ENABLED =false;


    @Config(confKey = "eggroll.core.command.default.serdes.type")
    public static String CONFKEY_CORE_COMMAND_DEFAULT_SERDES_TYPE ;
    @Config(confKey = "eggroll.core.log.dir")
    public static String CONFKEY_CORE_LOG_DIR ;

    @Config(confKey = "eggroll.core.stats.direct.memory.metrics", pattern = Dict.BOOLEAN_PATTERN)
    public static Boolean EGGROLL_CORE_STATS_DIRECT_MEMORY_METRICS =false;
    @Config(confKey = "eggroll.core.stats.direct.memory.metrics.interval.sec", pattern = Dict.POSITIVE_INTEGER_PATTERN)
    public static Integer EGGROLL_CORE_STATS_DIRECT_MEMORY_METRICS_INTERVAL_SEC = 60;



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
