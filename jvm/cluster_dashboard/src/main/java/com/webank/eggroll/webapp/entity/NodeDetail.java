package com.webank.eggroll.webapp.entity;

import lombok.*;

import java.util.Date;
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class NodeDetail {

    private Long resourceId;

    private Long serverNodeId;

    private String resourceType;

    private Long total;

    private Long used;

    private Long preAllocated;

    private Long allocated;

    private String extention;

    private String serverStatus;

    private String resourceStatus;

    private String name;

    private Long serverClusterId;

    private String host;

    private Integer port;

    private String nodeType;

    private Date lastHeartbeatAt;

}
