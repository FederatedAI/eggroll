package com.eggroll.core.pojo;

import lombok.Data;

@Data
public class SessionRanksTemp {

    private Long serverNodeId;

    private Long containerId;

    private Integer globalRank;

    private Integer localRank;

    private Integer index;

    public SessionRanksTemp(Long serverNodeId, Long containerId,Integer globalRank,Integer localRank,Integer index) {
        this.serverNodeId = serverNodeId;
        this.containerId = containerId;
        this.globalRank = globalRank;
        this.localRank = localRank;
        this.index = index;
    }
}
