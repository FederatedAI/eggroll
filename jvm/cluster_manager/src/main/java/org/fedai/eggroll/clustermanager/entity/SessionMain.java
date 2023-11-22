package org.fedai.eggroll.clustermanager.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.fedai.eggroll.core.pojo.ErSessionMeta;

import java.util.Date;

@TableName(value = "session_main", autoResultMap = true)
@Data
@AllArgsConstructor
public class SessionMain {
    @TableId(type = IdType.INPUT)
    private String sessionId;

    private String name;

    private String status;

    private String beforeStatus;

    private String statusReason;

    private String tag;

    private Integer totalProcCount;

    private Integer activeProcCount;

    private Date createdAt;

    private Date updatedAt;

    public SessionMain(String sessionId, String name, String status, String tag, Integer totalProcCount) {
        this.sessionId = sessionId;
        this.name = name;
        this.status = status;
        this.tag = tag;
        this.totalProcCount = totalProcCount;
    }

    public SessionMain(String sessionId, String name, String status, String tag, Integer totalProcCount, Integer activeProcCount, Date createdAt, Date updatedAt) {
        this.sessionId = sessionId;
        this.name = name;
        this.status = status;
        this.tag = tag;
        this.totalProcCount = totalProcCount;
        this.activeProcCount = activeProcCount;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public SessionMain() {
        super();
    }

    public ErSessionMeta toErSessionMeta() {
        ErSessionMeta result = new ErSessionMeta();
        result.setId(this.sessionId);
        result.setName(this.name);
        result.setTag(this.tag);
        result.setCreateTime(createdAt);
        result.setUpdateTime(updatedAt);
        if (StringUtils.isNotEmpty(status)) {
            result.setStatus(status);
        }
        result.setTotalProcCount(totalProcCount);
        result.setActiveProcCount(activeProcCount);
        return result;

    }
}