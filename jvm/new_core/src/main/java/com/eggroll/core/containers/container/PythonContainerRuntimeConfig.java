package com.eggroll.core.containers.container;

import com.eggroll.core.config.ErConf;
import com.eggroll.core.pojo.StaticErConf;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.function.Supplier;

public class PythonContainerRuntimeConfig extends ErConf {
    private final Map<String, String> options;
    Properties conf = ErConf.getConf();

    public PythonContainerRuntimeConfig(Map<String, String> options) {
        this.options = new HashMap<>(options);
        StaticErConf staticErConf = new StaticErConf();
        Map<String, String> props = staticErConf.getAll();

        for (Map.Entry<String, String> entry : props.entrySet()) {
            this.conf.put(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, String> entry : options.entrySet()) {
            this.conf.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public String getModuleName() {
        return "PythonContainerRuntimeConfig";
    }

    @Override
    public int getPort() {
        // 写入具体的端口号逻辑
        throw new UnsupportedOperationException("Not implemented");
    }

    public String getPythonExec(String key) {
//        List<Supplier<Optional<String>>> fallbacks = new ArrayList<>();
//
//        fallbacks.add(() -> Optional.ofNullable(key)
//                .filter(s -> !s.isEmpty())
//                .map(k -> this.getString(k,""))
//        );
//
//        fallbacks.add(() -> Optional.ofNullable(this.getString(Container.ContainerKey.PYTHON_EXEC,"")));
//
//        fallbacks.add(() -> Optional.ofNullable(this.getString(Container.ContainerKey.EGGPAIR_VENV,""))
//                .filter(s -> !s.isEmpty())
//                .map(v -> v + "/bin/python")
//        );
//
//        for (Supplier<Optional<String>> f : fallbacks) {
//            Optional<String> value = f.get();
//            if (value.isPresent()) {
//                return value.get();
//            }
//        }
        String value = getString(key, null);
        if (StringUtils.isNotBlank(value)) {
            return value;
        }
        value = getString(Container.ContainerKey.PYTHON_EXEC, null);
        if (StringUtils.isNotBlank(value)) {
            return value;
        }
        value = getString(Container.ContainerKey.EGGPAIR_VENV, null);
        if (StringUtils.isNotBlank(value)) {
            return value + "/bin/python";
        }
        throw new RuntimeException("python exec not found for key: " + key);
//        for (String fallback : fallbacks) {
//            String value = fallback;
//            if (value != null && !value.isEmpty()) {
//                return value;
//            }
//        }
//
//        throw new RuntimeException("python exec not found for key: " + key);
    }
}
