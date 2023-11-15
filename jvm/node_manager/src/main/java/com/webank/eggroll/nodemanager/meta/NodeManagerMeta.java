package com.webank.eggroll.nodemanager.meta;

import com.eggroll.core.config.Dict;
import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.constant.StringConstants;
import com.eggroll.core.utils.FileSystemUtils;
import com.eggroll.core.utils.JsonUtil;
import com.eggroll.core.utils.NetUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class NodeManagerMeta {

    private static Logger logger = LoggerFactory.getLogger(NodeManagerMeta.class);
    public static String status = Dict.INIT;
    public static Long serverNodeId = -1L;
    public static Long clusterId = -1L;
    public static String ip = MetaInfo.CONFKEY_NODE_MANAGER_HOST == null ? NetUtils.getLocalHost(MetaInfo.CONFKEY_NODE_MANAGER_NET_DEVICE) : MetaInfo.CONFKEY_NODE_MANAGER_HOST;
    public static Integer port = MetaInfo.CONFKEY_NODE_MANAGER_PORT;

    public static void refreshServerNodeMetaIntoFile() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentDateStr = sdf.format(new Date());

        Map<String, Object> map = new HashMap<>();
        map.put(Dict.KEY_SERVER_NODE_ID, serverNodeId);
        map.put(Dict.KEY_CLUSTER_ID, clusterId);
        map.put(Dict.KEY_UPDATE_TIME, currentDateStr);
        map.put(Dict.KEY_NODE_IP, ip);
        map.put(Dict.KEY_NODE_PORT, port.toString());

        Gson gson = new Gson();
        String content = gson.toJson(map);
        try {
            FileSystemUtils.fileWriter(getFilePath(), content);
        } catch (IOException e) {
            logger.error("refreshServerNodeMetaIntoFile failed: {}", e.getMessage());
        }

    }

    public static void loadNodeManagerMetaFromFile() {
        if (new File(getFilePath()).exists()) {
            String confNodeHost = MetaInfo.CONFKEY_NODE_MANAGER_HOST == null ? NetUtils.getLocalHost(MetaInfo.CONFKEY_NODE_MANAGER_NET_DEVICE) : MetaInfo.CONFKEY_NODE_MANAGER_HOST;
            int confNodePort = MetaInfo.CONFKEY_NODE_MANAGER_PORT;
            try {
                String content = FileSystemUtils.fileReader(getFilePath());
                logger.info("===========load node manager meta {}============", content);
                Map<String, Object> map = JsonUtil.json2Object(content, new TypeReference<Map<String, Object>>() {
                });
                if(map == null){
                    return;
                }
                String dictHost = String.valueOf( map.get(Dict.KEY_NODE_IP));
                int dictPort = Integer.parseInt((String) map.get(Dict.KEY_NODE_PORT));
                if (confNodeHost.equals(dictHost) && dictPort == confNodePort) {
                    serverNodeId = Long.valueOf((Integer) map.get(Dict.KEY_SERVER_NODE_ID));
                    clusterId = Long.valueOf((Integer) map.get(Dict.KEY_CLUSTER_ID));
                } else {
                    logger.info("load meta file , found invalid content : {}", content);
                }
            } catch (IOException e) {
                logger.error("loadNodeManagerMetaFromFile failed: {}", e.getMessage());
            }
        }
    }

    private static String getFilePath() {
        return MetaInfo.EGGROLL_DATA_DIR + StringConstants.SLASH + "NodeManagerMeta";
    }
}
