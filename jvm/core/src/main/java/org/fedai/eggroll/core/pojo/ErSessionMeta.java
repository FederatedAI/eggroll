package org.fedai.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.fedai.eggroll.core.constant.SessionStatus;
import org.fedai.eggroll.core.constant.StringConstants;
import org.fedai.eggroll.core.utils.JsonUtil;

import java.util.*;
import java.util.stream.Collectors;

@Data
public class ErSessionMeta implements RpcMessage {


    private String id = StringConstants.EMPTY;
    private String name = StringConstants.EMPTY;
    private String status = null;
    private String beforeStatus;
    private String statusReason;
    private Integer totalProcCount = null;
    private Integer activeProcCount = null;
    private String tag = StringConstants.EMPTY;
    private List<ErProcessor> processors = new ArrayList<>();
    private Date createTime = null;
    private Date updateTime = null;
    private Map<String, String> options = new HashMap<>();

    public ErSessionMeta() {

    }

    public ErSessionMeta(String id, String name, List<ErProcessor> processors, Integer totalProcCount, String status) {
        this.id = id;
        this.name = name;
        this.processors = processors;
        this.totalProcCount = totalProcCount;
        this.status = status;
    }

    public Meta.SessionMeta toProto() {
        Meta.SessionMeta.Builder builder = Meta.SessionMeta.newBuilder();
        builder.setId(this.id);
        builder.setName(this.name);
        if (status != null) {
            builder.setStatus(status);
        }
        builder.setTag(this.tag);
        builder.addAllProcessors(this.processors.stream().map(ErProcessor::toProto).collect(Collectors.toList()));
        if (options.size() > 0) {
            builder.putAllOptions(this.options);
        }
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
            this.options = sessionMeta.getOptionsMap();
            this.tag = sessionMeta.getTag();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean isOverState() {
        return SessionStatus.valueOf(this.status).isOver;
    }

}