package com.webank.ai.eggroll.core.utils;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Properties;

@Component
@Scope("prototype")
public interface PropertyGetter {
    public boolean addSource(Properties prop);
    public List<Properties> getAllSources();
    public String getProperty(String key);
    public String getProperty(String key, String defaultValue);
    public String getPropertyWithTemporarySource(String key, Properties ... props);
    public String getPropertyWithTemporarySource(String key, String defaultValue, Properties ... props);
}
