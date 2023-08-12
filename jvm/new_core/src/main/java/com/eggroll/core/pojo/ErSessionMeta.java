package com.eggroll.core.pojo;

import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.constant.StringConstants;
import com.eggroll.core.utils.JsonUtil;
import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
public class ErSessionMeta implements RpcMessage {

    Logger log = LoggerFactory.getLogger(ErSessionMeta.class);
    private String id = StringConstants.EMPTY;
    private String name = StringConstants.EMPTY;
    private String status = null;
    private Integer totalProcCount = null;
    private Integer activeProcCount = null;
    private String tag = StringConstants.EMPTY;
    private List<ErProcessor> processors = new ArrayList<>();
    private Date createTime = null;
    private Date updateTime = null;
    private Map<String, String> options = new HashMap<>();


    public Meta.SessionMeta toProto() {
        Meta.SessionMeta.Builder builder = Meta.SessionMeta.newBuilder();
        builder.setId(this.id);
        builder.setName(this.name);
        if (status != null) {
            builder.setStatus(status);
        }
        builder.setTag(this.tag);
        builder.addAllProcessors(this.processors.stream().map(ErProcessor::toProto).collect(Collectors.toList()));
        return builder.build();
    }

    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    @Override
    public String toString() {
        return JsonUtil.object2Json(this);
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.SessionMeta sessionMeta = Meta.SessionMeta.parseFrom(data);
            this.processors = sessionMeta.getProcessorsList().stream().map(ErProcessor::fromProto).collect(Collectors.toList());
            this.status = sessionMeta.getStatus();
            this.name = sessionMeta.getName();
            this.id = sessionMeta.getId();
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }

    public boolean isOverState() {
        if (StringUtils.equalsAny(this.status, SessionStatus.KILLED.name(), SessionStatus.CLOSED.name(), SessionStatus.ERROR.name())) {
            return true;
        } else {
            return false;
        }

    }

}