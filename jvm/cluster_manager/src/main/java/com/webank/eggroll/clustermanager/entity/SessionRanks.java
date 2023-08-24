package com.webank.eggroll.clustermanager.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

@TableName(value = "session_ranks", autoResultMap = true)
@Data
public class SessionRanks {
    @TableId(type = IdType.AUTO)
    private Long containerId;

    private String sessionId;

    private Long serverNodeId;

    private Integer globalRank;

    private Integer localRank;

    public SessionRanks(Long containerId, String sessionId, Long serverNodeId, Integer globalRank, Integer localRank) {
        this.containerId = containerId;
        this.sessionId = sessionId;
        this.serverNodeId = serverNodeId;
        this.globalRank = globalRank;
        this.localRank = localRank;
    }

    public SessionRanks() {
        super();
    }

}