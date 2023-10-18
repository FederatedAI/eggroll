package com.webank.eggroll.webapp.queryobject;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class SessionProcessorQO {

    private String sessionId;
    private Integer serverNodeId;
    private String processorId;
    private String status;
    private String createTime;
    private String updateTime;

    private Integer pageNum = 1;
    private Integer pageSize = 10;

}
