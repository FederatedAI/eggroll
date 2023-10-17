package com.webank.eggroll.webapp.queryobject;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServerNodeQO {

    private String serverNodeId;
    private String name;
    private String serverClusterId;
    private String nodeType;
    private String status;
    private String lastHeartbeatAt;

    private Integer pageNum = 1;
    private Integer pageSize = 10;
}
