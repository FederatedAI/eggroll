package org.fedai.eggroll.core.config;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ExtendEnvConf {

    static Logger logger = LoggerFactory.getLogger(MetaInfo.class);
    public static Map<String, String> confMap = new HashMap<>();

    public static void initToMap(Properties properties) {
        properties.forEach((k, v) -> {
            confMap.put(k.toString(), v.toString());
            logger.info("============ init extendConfig, key: {}, value: {} add to confMap =============", k.toString(), v.toString());
        });
    }

}
