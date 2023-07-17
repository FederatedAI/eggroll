package com.webank.eggroll.clustermanager.entity.scala;

import com.webank.eggroll.core.constant.StringConstants;
import lombok.Data;

import java.util.*;

@Data
public class ErSessionMeta_JAVA implements MetaRpcMessage_JAVA {
    private String id = StringConstants.EMPTY();
    private String name = StringConstants.EMPTY();
    private String status = StringConstants.EMPTY();
    private Integer totalProcCount = 0;
    private Integer activeProcCount = 0;
    private String tag = StringConstants.EMPTY();
    private List<ErProcessor_JAVA> processors = new ArrayList<>();
    private Date createTime = null;
    private Date updateTime = null;
    private Map<String, String> options = new HashMap<>();

}