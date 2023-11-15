package com.eggroll.core.containers.container;

import com.eggroll.core.config.MetaInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class PythonContainerRuntimeConfig {
    Map<String, String> options;

    public PythonContainerRuntimeConfig(Map<String, String> options) {
        this.options = options;
    }


    public String getPythonExec(String key) {
        String value = null;
        if (options != null) {
            value = options.get(key);
        }

        if (StringUtils.isBlank(value)) {
            value = MetaInfo.EGGROLL_CONTAINER_PYTHON_EXEC;
        }

        if (StringUtils.isBlank(value)) {
            value = MetaInfo.CONFKEY_RESOURCE_MANAGER_BOOTSTRAP_EGG_PAIR_VENV;
            if (StringUtils.isBlank(value)) {
                throw new RuntimeException("python exec not found for key: " + key);
            }
            value += "/bin/python";
        }
        return value;
    }
}
