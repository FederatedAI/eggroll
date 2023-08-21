package com.eggroll.core.pojo;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import com.webank.eggroll.core.meta.Containers;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class StopContainersRequest {

    Logger log = LoggerFactory.getLogger(StopContainersRequest.class);

    private String sessionId;
    private Long[] containers;

    public StopContainersRequest(String sessionId, Long[] containers) {
        this.sessionId = sessionId;
        this.containers = containers;
    }

    public StopContainersRequest deserialize(ByteString byteString) {
        Containers.StopContainersRequest proto = null;
        try {
            proto = Containers.StopContainersRequest.parseFrom(byteString);
        } catch (InvalidProtocolBufferException e) {
            log.error("StopContainersRequest.deserialize() error :" ,e);
        }
        List<Long> containerIdsList = (List<Long>) proto.getContainerIdsList().stream()
                .map(Long::valueOf)
                .collect(Collectors.toList());
        Long[] containerIds = containerIdsList.toArray(new Long[0]);
        return new StopContainersRequest(proto.getSessionId(), containerIds);
    }

    public ByteString serialize(StopContainersRequest src) {
        Containers.StopContainersRequest.Builder builder = Containers.StopContainersRequest.newBuilder();
        builder.setSessionId(src.getSessionId());

        List<Long> containerIds = (List<Long>) Arrays.stream(src.getContainers())
                .collect(Collectors.toList());
        builder.addAllContainerIds(containerIds);
        return builder.build().toByteString();
    }
}