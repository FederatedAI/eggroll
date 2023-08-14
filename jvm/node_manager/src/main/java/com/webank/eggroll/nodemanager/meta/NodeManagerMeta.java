package com.webank.eggroll.nodemanager.meta;

import com.eggroll.core.config.Dict;
import com.eggroll.core.constant.StringConstants;
import com.eggroll.core.utils.FileSystemUtils;
import com.eggroll.core.utils.JsonUtil;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NodeManagerMeta {

    private static Logger logger = LoggerFactory.getLogger(NodeManagerMeta.class);
    public static String status = Dict.INIT;
    public static Long serverNodeId = -1L;
    public static Long clusterId = -1L;

    public static void refreshServerNodeMetaIntoFile() {
        List<Long> list = new ArrayList<>();
        list.add(serverNodeId);
        list.add(clusterId);
        Gson gson = new Gson();
        String content = gson.toJson(list);
        try {
            FileSystemUtils.fileWriter(getFilePath(),content);
        } catch (IOException e) {
            logger.error("refreshServerNodeMetaIntoFile failed: {}", e.getMessage());
        }

    }

    public static void loadNodeManagerMetaFromFile() {
        try {
            String content = FileSystemUtils.fileReader(getFilePath());
            Gson gson = new Gson();
            logger.info("load node manager meta {}",content);
            List<Integer> list = JsonUtil.json2Object(content, List.class);
            serverNodeId = list.get(0).longValue();
            clusterId = list.get(1).longValue();
        } catch (IOException e) {
            logger.error("loadNodeManagerMetaFromFile failed: {}", e.getMessage());
        }

    }

    private static String getFilePath() {
        return Dict.EGGROLL_DATA_DIR + StringConstants.SLASH + "NodeManagerMeta";
    }
}
