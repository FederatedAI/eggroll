package com.eggroll.core.pojo;


import com.webank.eggroll.core.meta.Containers;
import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@Data
public class DownloadJobResponse implements RpcMessage{

    Logger log = LoggerFactory.getLogger(DownloadJobResponse.class);

    private String sessionId;
    private List<ContainerContent> containerContents;

    public DownloadJobResponse(String sessionId, List<ContainerContent> containerContents) {
        this.sessionId = sessionId;
        this.containerContents = containerContents;
    }

    public Deepspeed.DownloadJobResponse toProto() {
        Deepspeed.DownloadJobResponse.Builder builder = Deepspeed.DownloadJobResponse.newBuilder();
        builder.setSessionId(sessionId);
        if (CollectionUtils.isNotEmpty(containerContents)) {
            List<Containers.ContainerContent> contentList = new ArrayList<>();
            containerContents.forEach(containerContent -> {
                contentList.add(containerContent.toProto());
            });
            builder.addAllContainerContent(contentList);
        }
        return builder.build();
    }

    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Deepspeed.DownloadJobResponse src =  Deepspeed.DownloadJobResponse.parseFrom(data);
            this.sessionId = src.getSessionId();
            List<Containers.ContainerContent> containerContentList = src.getContainerContentList();
            if (CollectionUtils.isNotEmpty(containerContentList)) {
               List<ContainerContent> tempList = new ArrayList<>();
                containerContentList.forEach(containerContent -> {
                    ContainerContent content = new ContainerContent(containerContent.getContent().toByteArray(),containerContent.getCompressMethod());
                    tempList.add(content);
                });
                this.containerContents = tempList;
            }
        }catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}