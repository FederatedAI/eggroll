package com.webank.eggroll.clustermanager.command;

import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class CommandURI {
    private String uriString;
    private URI uri;
    private String queryString;
    private Map<String, String> queryPairs;

    public CommandURI(String uriString) throws UnsupportedEncodingException {
        this.uriString = uriString;
        this.uri = URI.create(uriString);
        this.queryString = uri.getQuery();
        this.queryPairs = new HashMap<>();

        if (StringUtils.isBlank(queryString)) {
            queryPairs.put("route", uriString);
        } else {
            for (String pair : queryString.split("&")) {
                int idx = pair.indexOf("=");
                String key = (idx > 0) ? URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8.name()) : pair;
                String value = (idx > 0 && pair.length() > idx + 1) ? URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8.name()) : "";
                queryPairs.put(key, value);
            }
        }
    }

    public CommandURI(ErCommandRequest src) {
        this(src.getUri());
    }

    public CommandURI(String prefix, String name) {
        this(prefix + "/" + name);
    }

    public String getName() {
        return StringUtils.substringAfterLast(uri.getPath(), "/");
    }

    public String getQueryValue(String key) {
        return queryPairs.get(key);
    }

    public String getRoute() {
        return queryPairs.get("route");
    }
}