package com.webank.eggroll.clustermanager.config;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public abstract class ErConf_JAVA {
    protected Properties conf = new Properties();
    private Map<String, String> confRepository = new HashMap<>();

    public Properties getProperties() {
        Properties duplicateConf = new Properties();
        duplicateConf.putAll(getConf());
        return duplicateConf;
    }

    public String getProperty(String key, Object defaultValue) {
        return getProperty(key,defaultValue,false);
    }

    public String getProperty(String key, Object defaultValue, boolean forceReload) {
        String result = null;
        String value = confRepository.get(key);

        if (forceReload || value == null) {
            Object resultRef = getConf().get(key);

            if (resultRef != null) {
                result = resultRef.toString();
                confRepository.put(key, result);
            } else {
                result = defaultValue.toString();
            }
        } else {
            result = value;
        }

        return result;
    }

    public long getLong(String key, long defaultValue) {
        return Long.parseLong(getProperty(key, defaultValue));
    }

    public int getInt(String key, int defaultValue) {
        return Integer.parseInt(getProperty(key, defaultValue));
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        return Boolean.parseBoolean(getProperty(key, defaultValue));
    }

    public String getString(String key, String defaultValue) {
        return getProperty(key, defaultValue);
    }

    public static int getPort() {
        return 0;
    }

    public static String getModuleName() {
        return null;
    }

    public void addProperties(Properties prop) {
        conf.putAll(prop);
    }

    public void addProperties(String path) throws IOException {
        Properties prop = new Properties();

        File current = new File(".");
        System.out.println("current dir: " + current.getAbsolutePath());
        System.out.println("read config file: " + path);

        try (InputStream fis = new FileInputStream(path);
             BufferedInputStream bis = new BufferedInputStream(fis)) {
            prop.load(bis);
        }

        addProperties(prop);
    }

    public void addProperty(String key, String value) {
        conf.setProperty(key, value);
    }

    public <T> T get(String key, T defaultValue) {
        Object result = getConf().get(key);

        if (result != null) {
            return (T) result;
        } else {
            return defaultValue;
        }
    }

    public Map<String, String> getAll() {
        Map<String, String> result = new HashMap<>();
        Properties props = getConf();

        for (String key : props.stringPropertyNames()) {
            result.put(key, props.getProperty(key));
        }

        return result;
    }

    protected Properties getConf() {
        return conf;
    }
}



