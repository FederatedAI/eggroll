package com.eggroll.core.pojo;

import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.constant.StringConstants;
import com.eggroll.core.utils.JsonUtil;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

@Data
public class ErSessionMeta implements RpcMessage {


    private String id = StringConstants.EMPTY;
    private String name = StringConstants.EMPTY;
    private String  status = null;
    private Integer totalProcCount = null;
    private Integer activeProcCount = null;
    private String tag = StringConstants.EMPTY;
    private List<ErProcessor> processors = new ArrayList<>();
    private Date createTime = null;
    private Date updateTime = null;

    @Override
    public byte[] serialize() {
        Meta.SessionMeta.Builder  builder = Meta.SessionMeta.newBuilder();
        builder.setId(this.id);
        builder.setName(this.name);
        if(status!=null){
            builder.setStatus(status);
        }
        for(ErProcessor erProcessor:processors){
            builder.addProcessors(erProcessor.toProto());
        }
        builder.setTag(this.tag);
        return builder.build().toByteArray();

    }

    @Override
    public String toString() {
        return JsonUtil.object2Json(this);
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.SessionMeta  sessionMeta =  Meta.SessionMeta.parseFrom(data);
            List<Meta.Processor> processorsList = sessionMeta.getProcessorsList();
            for (Meta.Processor meataProcessor:processorsList) {
                ErProcessor erProcessor = new ErProcessor();
                erProcessor.deserialize(meataProcessor.toByteArray());
                this.processors.add(erProcessor);
            }
            this.status = sessionMeta.getStatus();
            this.setName(sessionMeta.getName());
            this.setId(sessionMeta.getId());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    public   boolean isOverState(){
        if (StringUtils.equalsAny(this.status, SessionStatus.KILLED.name(), SessionStatus.CLOSED.name(), SessionStatus.ERROR.name())){
            return true;
        }else
        {
            return false;
        }

    }

}