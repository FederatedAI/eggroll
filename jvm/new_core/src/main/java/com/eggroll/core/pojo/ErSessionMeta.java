package com.eggroll.core.pojo;

import com.eggroll.core.constant.StringConstants;
import lombok.Data;

import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class ErSessionMeta  {
    private String id = StringConstants.EMPTY;
    private String name = StringConstants.EMPTY;
    private String status = StringConstants.EMPTY;
    private Integer totalProcCount = 0;
    private Integer activeProcCount = 0;
    private String tag = StringConstants.EMPTY;
    private List<ErProcessor> processors = new ArrayList<>();
    private Date createTime = null;
    private Date updateTime = null;
    private Map<String, String> options = new HashMap<>();

}