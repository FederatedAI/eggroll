package org.fedai.eggroll.webapp.entity;


import lombok.*;

import java.util.Date;


@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class NodeDetail {// 节点详情

    private Long resourceId;

    private Long serverNodeId;

    private String resourceType;

    private Long total;

    private Long used;

    private Long preAllocated;

    private Long allocated;

    private String extention;

    private String status;

    private Date createdAt;

    private Date updatedAt;

}
