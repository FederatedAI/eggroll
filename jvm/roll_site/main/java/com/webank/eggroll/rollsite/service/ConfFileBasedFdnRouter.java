/*
 * Copyright 2019 The Eggroll Authors. All Rights Reserved.
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

package com.webank.eggroll.rollsite.service;

import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.api.networking.proxy.Proxy;
import com.webank.eggroll.core.util.ToStringUtils;
import com.webank.eggroll.rollsite.model.ProxyServerConf;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ConfFileBasedFdnRouter implements FdnRouter {
    private static final String IP = "ip";
    private static final String PORT = "port";
    private static final String HOSTNAME = "hostname";
    private static final String DEFAULT = "default";
    private static final Logger LOGGER = LogManager.getLogger(ConfFileBasedFdnRouter.class);
    private final BasicMeta.Endpoint emptyEndpoint;
    @Autowired
    private ProxyServerConf proxyServerConf;
    private Map<String, Map<String, List<BasicMeta.Endpoint>>> routeTable;
    private Map<Proxy.Topic, BasicMeta.Endpoint> topicEndpointMapping;
    private BasicMeta.Endpoint.Builder endpointBuilder;
    private JsonObject confJson;
    private Set<String> routeNeighbours;
    private Set<BasicMeta.Endpoint> intranetEndpoints;
    private JsonParser jsonParser;
    private Map<Proxy.Topic, Set<Proxy.Topic>> allow;
    private Map<Proxy.Topic, Set<Proxy.Topic>> deny;
    private boolean defaultAllow;
    private String routeTableFilename;
    private SecureRandom random;

    public ConfFileBasedFdnRouter() {
        routeTable = new ConcurrentHashMap<>();
        topicEndpointMapping = new ConcurrentHashMap<>();
        endpointBuilder = BasicMeta.Endpoint.newBuilder();
        routeNeighbours = new HashSet<>();
        intranetEndpoints = new HashSet<>();

        allow = new ConcurrentHashMap<>();
        deny = new ConcurrentHashMap<>();
        defaultAllow = false;

        random = new SecureRandom();
        emptyEndpoint = endpointBuilder.build();
    }

    public ConfFileBasedFdnRouter(String filename) {
        this();
        setRouteTable(filename);
    }

    @Override
    public synchronized void setRouteTable(String filename) {
        if (StringUtils.isBlank(routeTableFilename)) {
            LOGGER.debug("setting routeTable path={}", filename);
            this.routeTableFilename = filename;
            init();
        } else {
            LOGGER.debug("trying to reset routeTable path. current path={}, new path={}",
                    routeTableFilename, filename);
        }
    }

    public static String format(String jsonStr){
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(jsonStr.getBytes());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            char ch;
            int read;
            int space=0;
            while((read = in.read()) > 0){
                ch = (char)read;
                switch (ch){
                    case '{': {
                        space = outputAndRightMove(space, ch, out);
                        break;
                    }
                    case '[': {
                        out.write(ch);
                        space += 2;
                        break;
                    }
                    case '}': {
                        space = outputAndLeftMove(space, ch, out);
                        break;
                    }
                    case ']': {
                        space = outputAndLeftMove(space, ch, out);
                        break;
                    }
                    case ',': {
                        out.write(ch);
                        outputNewline(out);
                        out.write(getBlankingStringBytes(space));
                        break;
                    }
                    default: {
                        out.write(ch);
                        break;
                    }
                }
            }
            return out.toString();
        } catch (IOException e){
            e.printStackTrace();
        }

        return null;
    }

    public static int outputAndRightMove(int space, char ch, ByteArrayOutputStream out) throws IOException {
        outputNewline(out);
        out.write(getBlankingStringBytes(space));
        out.write(ch);
        outputNewline(out);
        space += 2;
        out.write(getBlankingStringBytes(space));
        return space;
    }

    public static int outputAndLeftMove(int space, char ch, ByteArrayOutputStream out) throws IOException{
        outputNewline(out);
        space -= 2;
        out.write(getBlankingStringBytes(space));
        out.write(ch);
        return space;
    }

    public static byte[] getBlankingStringBytes(int space){
        StringBuilder sb = new StringBuilder("");
        for (int i = 0; i < space; i++) {
            sb.append(" ");
        }
        return sb.toString().getBytes();
    }

    public static void outputNewline(ByteArrayOutputStream out){
        out.write('\n');
    }


    @Override
    public void initRouteTableFile(String routeTablePath) {
        JSONObject result = new JSONObject();
        JSONObject seg0 = new JSONObject();
        JSONObject seg1 = new JSONObject();
        JSONObject seg2 = new JSONObject();
        JSONObject default_allow = new JSONObject();
        seg0.put("ip", "127.0.0.1");
        seg0.put("port", "9999");

        JSONArray jsonArray = new JSONArray();
        jsonArray.put(0, seg0);

        seg1.put("default", jsonArray);
        seg2.put("default", seg1);

        default_allow.put("default_allow", true);

        result.put("route_table", seg2);
        result.put("permission", default_allow);

        String jsonString = format(result.toString());
        try {
            System.out.println("routeTablePath:" + routeTablePath);
            File file = new File(routeTablePath);
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            if (file.exists()) {
                file.delete();
            }
            file.createNewFile();

            Writer write = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
            write.write(jsonString);
            write.flush();
            write.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        /*
        File file = new File(routeTablePath);
        if(file.exists()) {
            file.delete();
        }
        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        FileWriter fw = null;
        try {
            fw = new FileWriter(file.getAbsoluteFile());
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedWriter bw = new BufferedWriter(fw);
        result.write(bw);
        try {
            bw.flush();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        */
        System.out.println("end initRouteTable");
    }


    @Override
    public void updateRouteTable() {
        init();
    }

    @Override
    public boolean isAllowed(Proxy.Topic from, Proxy.Topic to) {
        if (hasRule(deny, from, to)) {
            return false;
        } else if (hasRule(allow, from, to)) {
            return true;
        } else {
            return defaultAllow;
        }
    }

    @Override
    public boolean isIntranet(BasicMeta.Endpoint endpoint) {
        return intranetEndpoints.contains(endpoint);
    }

    public void init() {
        jsonParser = new JsonParser();

        JsonReader jsonReader = null;
        try {
            jsonReader = new JsonReader(new FileReader(routeTableFilename.replaceAll("\\.\\./", "")));
            confJson = jsonParser.parse(jsonReader).getAsJsonObject();
        } catch (FileNotFoundException e) {
            LOGGER.error("File not found. path={}", routeTableFilename);
            throw new RuntimeException(e);
        } finally {
            if (jsonReader != null) {
                try {
                    jsonReader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        initRouteTable(confJson.getAsJsonObject("route_table"));
        initPermission(confJson.getAsJsonObject("permission"));

        LOGGER.debug("refreshed route table at={}", routeTableFilename);
    }

    @Override
    public BasicMeta.Endpoint route(Proxy.Topic topic) {
        Preconditions.checkNotNull(topic, "topic cannot be null");
        LOGGER.trace("routing to topic={}", ToStringUtils.toOneLineString(topic));

        BasicMeta.Endpoint result = topicEndpointMapping.getOrDefault(topic, null);

        // 1st priority: routed and fully match
        if (result != null) {
            return result;
        }

        String topicName = topic.getName();
        String coordinator = topic.getPartyId();
        String role = topic.getRole();

        // todo:2: add callback check
        BasicMeta.Endpoint callback = topic.getCallback();
        boolean overridden = false;

        // 2nd priority: callback
        if (callback != null && proxyServerConf.getCoordinator().equals(coordinator)) {
            String ip = callback.getIp();
            String hostname = callback.getHostname();

            if (!StringUtils.isAllBlank(ip, hostname)
                    && (routeNeighbours.contains(ip) || routeNeighbours.contains(hostname))) {
                result = callback;
                overridden = true;
            }
        }
        Proxy.Topic noCallbackTopic = null;
        // 3rd priority: routed endpoint
        if (!overridden) {
            noCallbackTopic = getNoCallbackTopic(topic);
            result = topicEndpointMapping.getOrDefault(noCallbackTopic, null);

            if (result != null) {
                overridden = true;
            }
        }

        // 4th priority: route table
        if (!overridden) {
            if (StringUtils.isAnyBlank(topicName, coordinator, role)) {
                throw new IllegalArgumentException("one of topic name, coordinator, role is null. topic: " + topic);
            }

            Map<String, List<BasicMeta.Endpoint>> roleTable =
                    routeTable.getOrDefault(coordinator, routeTable.getOrDefault(DEFAULT, null));
            if (roleTable == null) {
                /* throw new IllegalStateException("No available endpoint for the coordinator. " +
                        "Considering adding a default endpoint?");
                        */
                return null;
            }

            List<BasicMeta.Endpoint> endpoints =
                    roleTable.getOrDefault(role, roleTable.getOrDefault(DEFAULT, null));

            if (endpoints == null || endpoints.isEmpty()) {
                return null;
                /* throw new IllegalStateException("No available endpoint for this role. " +
                        "Considering adding a default endpoint, or check if the list is empty?");
                        */
            }

            // actual route algorithm, using a hash pattern now
            int len = endpoints.size();
            int hashCode = topic.hashCode();

            int pos = Math.abs(hashCode);
            if (pos == Integer.MIN_VALUE) {
                pos = 0;
            }

            pos = pos % len;

            LOGGER.trace("route: hashcode: {}, len: {}, pos: {}", topic.hashCode(), len, pos);

            result = endpoints.get(pos);
        }

/*        topicEndpointMapping.put(topic, result);
        if (noCallbackTopic != null && !topicEndpointMapping.containsKey(noCallbackTopic)) {
            topicEndpointMapping.put(noCallbackTopic, result);
        }*/

        return result;
    }

    private Proxy.Topic getNoCallbackTopic(Proxy.Topic topic) {
        Preconditions.checkNotNull(topic);

        Proxy.Topic.Builder topicBuilder = Proxy.Topic.newBuilder().mergeFrom(topic);
        Proxy.Topic result = topicBuilder.setCallback(emptyEndpoint).build();

        return result;
    }

    private void initRouteTable(JsonObject confJson) {
        Map<String, Map<String, List<BasicMeta.Endpoint>>> newRouteTable = new ConcurrentHashMap<>();
        Set<String> newRouteNeighbours = new HashSet<>();
        Set<BasicMeta.Endpoint> newIntranetEndpoints = new HashSet<>();

        boolean isIntranet = false;
        // loop through coordinator
        for (Map.Entry<String, JsonElement> coordinatorEntry : confJson.entrySet()) {
            String coordinatorKey = coordinatorEntry.getKey();
            JsonObject coordinatorValue = coordinatorEntry.getValue().getAsJsonObject();

            Map<String, List<BasicMeta.Endpoint>> roleTable = newRouteTable.get(coordinatorKey);
            if (roleTable == null) {
                roleTable = new ConcurrentHashMap<>(4);
                newRouteTable.put(coordinatorKey, roleTable);
            }

            if (coordinatorKey.equals(proxyServerConf.getCoordinator())
                    || coordinatorKey.equals(proxyServerConf.getPartyId())) {
                isIntranet = true;
            }

            // loop through role in coordinator
            for (Map.Entry<String, JsonElement> roleEntry : coordinatorValue.entrySet()) {
                String roleKey = roleEntry.getKey();
                JsonArray roleValue = roleEntry.getValue().getAsJsonArray();

                List<BasicMeta.Endpoint> endpoints = roleTable.get(roleKey);
                if (endpoints == null) {
                    endpoints = new ArrayList<>();
                    roleTable.put(roleKey, endpoints);
                }

                // loop through endpoints
                for (JsonElement endpointElement : roleValue) {
                    endpointBuilder.clear();
                    JsonObject endpointJson = endpointElement.getAsJsonObject();

                    if (endpointJson.has(IP)) {
                        String targetIp = endpointJson.get(IP).getAsString();
                        endpointBuilder.setIp(targetIp);
                        newRouteNeighbours.add(targetIp);
                    }

                    if (endpointJson.has(PORT)) {
                        int targetPort = endpointJson.get(PORT).getAsInt();
                        endpointBuilder.setPort(targetPort);
                    }

                    if (endpointJson.has(HOSTNAME)) {
                        String targetHostname = endpointJson.get(HOSTNAME).getAsString();
                        endpointBuilder.setHostname(targetHostname);
                        newRouteNeighbours.add(targetHostname);
                    }

                    BasicMeta.Endpoint endpoint = endpointBuilder.build();

                    endpoints.add(endpoint);
                    if (isIntranet) {
                        newIntranetEndpoints.add(endpoint);
                    }
                }
            }
            isIntranet = false;
        }

        routeTable = newRouteTable;
        routeNeighbours = newRouteNeighbours;
        intranetEndpoints = newIntranetEndpoints;
    }

    private void initPermission(JsonObject confJson) {
        boolean newDefaultAllow = false;
        Map<Proxy.Topic, Set<Proxy.Topic>> newAllow = new ConcurrentHashMap<>();
        Map<Proxy.Topic, Set<Proxy.Topic>> newDeny = new ConcurrentHashMap<>();

        if (confJson.has("default_allow")) {
            newDefaultAllow = confJson.getAsJsonPrimitive("default_allow").getAsBoolean();
        }

        if (confJson.has("allow")) {
            initPermissionType(newAllow, confJson.getAsJsonArray("allow"));
        }

        if (confJson.has("deny")) {
            initPermissionType(newDeny, confJson.getAsJsonArray("deny"));
        }

        defaultAllow = newDefaultAllow;
        allow = newAllow;
        deny = newDeny;
    }

    private void initPermissionType(Map<Proxy.Topic, Set<Proxy.Topic>> target, JsonArray conf) {
        for (JsonElement pairElement : conf) {
            JsonObject pair = pairElement.getAsJsonObject();

            JsonObject from = pair.getAsJsonObject("from");
            Proxy.Topic fromTopic = createTopicFromJson(from);

            JsonObject to = pair.getAsJsonObject("to");
            Proxy.Topic toTopic = createTopicFromJson(to);

            if (!target.containsKey(fromTopic)) {
                target.put(fromTopic, new HashSet<>());
            }
            Set<Proxy.Topic> toTopics = target.get(fromTopic);
            toTopics.add(toTopic);
        }
    }

    private Proxy.Topic createTopicFromJson(JsonObject json) {
        Proxy.Topic.Builder topicBuilder = Proxy.Topic.newBuilder();
        if (json.has("coordinator")) {
            topicBuilder.setPartyId(json.get("coordinator").getAsString());
        }

        if (json.has("role")) {
            topicBuilder.setRole(json.get("role").getAsString());
        }

        return topicBuilder.build();
    }

    private boolean hasRule(Map<Proxy.Topic, Set<Proxy.Topic>> target, Proxy.Topic from, Proxy.Topic to) {
        boolean result = false;

        if (target == null || target.isEmpty()) {
            return result;
        }

        Proxy.Topic.Builder fromBuilder = Proxy.Topic.newBuilder();
        Proxy.Topic.Builder toBuilder = Proxy.Topic.newBuilder();

        Proxy.Topic fromValidator = fromBuilder.setPartyId(from.getPartyId()).setRole(from.getRole()).build();
        Proxy.Topic toValidator = toBuilder.setPartyId(to.getPartyId()).setRole(to.getRole()).build();

        Set<Proxy.Topic> rules = null;
        if (target.containsKey(fromValidator)) {
            rules = target.get(fromValidator);
        }

        int stage = 0;
        while (stage < 3 && rules == null) {
            switch (stage) {
                case 0:
                    break;
                case 1:
                    fromValidator = fromBuilder.setRole("*").build();
                    break;
                case 2:
                    fromValidator = fromBuilder.setPartyId("*").build();
                    break;
                default:
                    throw new IllegalStateException("Illegal state when checking from rule");
            }

            if (target.containsKey(fromValidator)) {
                rules = target.get(fromValidator);
            }

            ++stage;
        }

        if (rules == null) {
            return result;      // false
        }


        stage = 0;
        while (stage < 3 && !result) {
            switch (stage) {
                case 0:
                    break;
                case 1:
                    toValidator = toBuilder.setRole("*").build();
                    break;
                case 2:
                    toValidator = toBuilder.setPartyId("*").build();
                    break;
                default:
                    throw new IllegalStateException("Illegal state when checking to rule");
            }

            if (rules.contains(toValidator)) {
                result = true;
            }

            ++stage;
        }

        return result;
    }
}
