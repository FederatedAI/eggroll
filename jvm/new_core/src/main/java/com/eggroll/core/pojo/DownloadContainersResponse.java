package com.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Containers;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@Data
public class DownloadContainersResponse implements RpcMessage {

    Logger log = LoggerFactory.getLogger(DownloadContainersResponse.class);

    private String sessionId;
    private List<Containers.ContainerContent> containerContents;


    public DownloadContainersResponse() {

    }

    public DownloadContainersResponse(String sessionId, List<ContainerContent> containerContentList) {
        this.sessionId = sessionId;
        containerContents = new ArrayList<>();
        containerContentList.forEach(containerContent -> {
            containerContents.add(containerContent.toProto());
        });
    }

    public Containers.DownloadContainersResponse toProto() {
        Containers.DownloadContainersResponse.Builder builder = Containers.DownloadContainersResponse.newBuilder();
        builder.setSessionId(this.sessionId);
        builder.addAllContainerContent(containerContents);
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
            this.containerContents = response.getContainerContentList();
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}

