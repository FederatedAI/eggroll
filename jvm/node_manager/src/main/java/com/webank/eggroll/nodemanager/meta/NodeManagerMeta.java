package com.webank.eggroll.nodemanager.meta;

import com.eggroll.core.config.Dict;
import com.eggroll.core.constant.StringConstants;
import com.eggroll.core.utils.FileSystemUtils;
import com.eggroll.core.utils.JsonUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NodeManagerMeta {

    private static Logger logger = LoggerFactory.getLogger(NodeManagerMeta.class);
    public static String status = Dict.INIT;
    public static Long serverNodeId = -1L;
    public static Long clusterId = -1L;

    public static void refreshServerNodeMetaIntoFile() {
        Map<String,Object> map = new HashMap<>();
        map.put(Dict.KEY_SERVER_NODE_ID,serverNodeId);
        map.put(Dict.KEY_CLUSTER_ID,clusterId);
        Gson gson = new Gson();
        String content = gson.toJson(map);
        try {
            FileSystemUtils.fileWriter(getFilePath(),content);
        } catch (IOException e) {
            logger.error("refreshServerNodeMetaIntoFile failed: {}", e.getMessage());
        }

    }

    public static void loadNodeManagerMetaFromFile() {
        try {
            String content = FileSystemUtils.fileReader(getFilePath());
            logger.info("load node manager meta {}",content);
            Map<String,Object> map = JsonUtil.json2Object(content, new TypeReference<Map<String, Object>>() {
            });
            serverNodeId = (Long)map.get(Dict.KEY_SERVER_NODE_ID);
            clusterId = (Long)map.get(Dict.KEY_CLUSTER_ID);
        } catch (IOException e) {
            logger.error("loadNodeManagerMetaFromFile failed: {}", e.getMessage());
        }

    }

    private static String getFilePath() {
        return Dict.EGGROLL_DATA_DIR + StringConstants.SLASH + "NodeManagerMeta";
    }
}
