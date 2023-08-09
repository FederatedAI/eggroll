package com.webank.eggroll.nodemanager.meta;

import com.eggroll.core.config.Dict;
import lombok.Data;

public class NodeManagerMeta {
    public static String status = Dict.INIT;
    public static Long serverNodeId = -1L;
    public static Long clusterId = -1L;

    //todo
    public void refreshServerNodeMetaIntoFile() {

    }

    //todo
    public void loadNodeManagerMetaFromFile() {

    }
}
