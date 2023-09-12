package com.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Containers;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@Data
public class DownloadContainersResponse implements RpcMessage {

    Logger log = LoggerFactory.getLogger(DownloadContainersResponse.class);

    private String sessionId;
    private List<ContainerContent> containerContents;


    public DownloadContainersResponse() {

    }

    public DownloadContainersResponse(String sessionId, List<ContainerContent> containerContents) {
        this.sessionId = sessionId;
        this.containerContents = containerContents;
    }

    public Containers.DownloadContainersResponse toProto() {
        Containers.DownloadContainersResponse.Builder builder = Containers.DownloadContainersResponse.newBuilder();
        builder.setSessionId(this.sessionId);

        List<Containers.ContainerContent> list = new ArrayList<>();
        containerContents.forEach(containerContent -> {
            list.add(containerContent.toProto());
        });
        builder.addAllContainerContent(list);
        return builder.build();
    }


    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Containers.DownloadContainersResponse response = Containers.DownloadContainersResponse.parseFrom(data);
            this.sessionId = response.getSessionId();
            List<Containers.ContainerContent> containerContentList = response.getContainerContentList();
            if (CollectionUtils.isNotEmpty(containerContentList)) {
                List<ContainerContent> containerContentsList = new ArrayList<>();
                containerContentList.forEach(containerContent -> {
                    ContainerContent content = new ContainerContent(containerContent.getContent().toByteArray(),containerContent.getCompressMethod());
                    containerContentsList.add(content);
                });
                this.containerContents = containerContentsList;
            }
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}

