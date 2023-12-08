package org.fedai.eggroll.webapp.entity;

import lombok.*;

import java.util.Date;


@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class NodeInfo {// 节点概况

    private Long resourceId;

    private Long serverNodeId;

    private String resourceType;

    private Long total;

    private Long allocated;

    private String serverNodeStatus;

    private String nodeResourceStatus;

    private String name;

    private Long serverClusterId;

    private String host;

    private Integer port;

    private String nodeType;

    private Date lastHeartbeatAt;

    private Date createdAt;

    private Date updatedAt;

}
