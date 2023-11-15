package org.fedai.eggroll.core.pojo;

import com.google.protobuf.ByteString;
import com.webank.eggroll.core.meta.Containers;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class ContainerContent implements RpcMessage {

    Logger log = LoggerFactory.getLogger(ContainerContent.class);

    private int rank;
    private byte[] content;
    private String compressMethod;

    public ContainerContent() {

    }

    public ContainerContent(byte[] content, String compressMethod) {
        this.content = content;
        this.compressMethod = compressMethod;
    }

    public ContainerContent(int rank, byte[] content, String compressMethod) {
        this.rank = rank;
        this.content = content;
        this.compressMethod = compressMethod;
    }

    public Containers.ContainerContent toProto() {
        return Containers.ContainerContent.newBuilder()
//                .setRank(src.rank)
                .setContent(ByteString.copyFrom(content))
                .setCompressMethod(compressMethod)
                .build();
    }

    public ContainerContent fromProto(Containers.ContainerContent content) {
        ContainerContent containerContent = new ContainerContent();
        containerContent.deserialize(content.toByteArray());
        return containerContent;
    }

    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Containers.ContainerContent src = Containers.ContainerContent.parseFrom(data);
            this.content = src.getContent().toByteArray();
            this.compressMethod = src.getCompressMethod();
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}
