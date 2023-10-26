package com.eggroll.core.pojo;

import com.eggroll.core.constant.StringConstants;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class CustomizedJobRequest {
    Logger log = LoggerFactory.getLogger(CustomizedJobRequest.class);

    // 定制类型: repeat(每个节点重复完成指定进程), noRepeat（所有节点共同完成指定进程）
    private String customType = StringConstants.EMPTY;

    private int processorSize;

    private String resourceMonitorType;

    // py脚本路径
    private String scriptPath = StringConstants.EMPTY;
    private String sessionId = StringConstants.EMPTY;
    private String name = StringConstants.EMPTY;
    private String jobType = StringConstants.EMPTY;
    private Integer worldSize = 0;
    private List<String> commandArguments = new ArrayList<>();
    private Map<String, String> environmentVariables = new HashMap<>();
    private Map<String, byte[]> files;
    private Map<String, byte[]> zippedFiles;
    private ResourceOptions resourceOptions;
    private Map<String, String> options = new HashMap<>();
}
