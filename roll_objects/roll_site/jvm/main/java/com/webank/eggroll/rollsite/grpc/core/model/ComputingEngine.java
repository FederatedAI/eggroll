package com.webank.eggroll.rollsite.grpc.core.model;

import com.google.common.base.Preconditions;
import com.webank.ai.eggroll.api.computing.ComputingBasic;
import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.eggroll.rollsite.grpc.core.constant.StringConstants;
import javax.annotation.concurrent.Immutable;
import org.apache.commons.lang3.StringUtils;

@Immutable
public class ComputingEngine {
    private final String host;
    private final int port;
    private final ComputingEngineType computingEngineType;
    private final Process process;

    public ComputingEngine(ComputingEngineType type) {
        this("localhost", -1, type, null);
    }

    public ComputingEngine(String host, int port) {
        this(host, port, ComputingEngineType.EGGROLL, null);
    }

    public ComputingEngine(String host, int port, Process process) {
        this(host, port, ComputingEngineType.EGGROLL, process);
    }

    public ComputingEngine(String host, int port, ComputingEngineType computingEngineType, Process process) {
        this.host = host;
        this.port = port;
        this.computingEngineType = computingEngineType;
        this.process = process;
    }

    public ComputingBasic.ComputingEngineDescriptor toProtobuf() {
        return ComputingBasic.ComputingEngineDescriptor.newBuilder()
                .setEndpoint(BasicMeta.Endpoint.newBuilder().setHostname(host).setPort(port).build())
                .setType(ComputingBasic.ComputingEngineType.EGGROLL)
                .build();
    }

    public static ComputingEngine fromProtobuf(ComputingBasic.ComputingEngineDescriptor pb) {
        Preconditions.checkNotNull(pb);

        BasicMeta.Endpoint endpoint = pb.getEndpoint();
        Preconditions.checkNotNull(endpoint);

        String host = endpoint.getIp();
        if (StringUtils.isBlank(host)) {
            host = endpoint.getHostname();
        }
        return new ComputingEngine(host, endpoint.getPort());
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public ComputingEngineType getComputingEngineType() {
        return computingEngineType;
    }

    public Process getProcess() {
        return process;
    }

    public static enum ComputingEngineType {
        EGGROLL(StringConstants.EGGROLL);

        private String name;

        ComputingEngineType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ComputingEngine)) return false;

        ComputingEngine that = (ComputingEngine) o;

        if (port != that.port) return false;
        if (host != null ? !host.equals(that.host) : that.host != null) return false;
        return computingEngineType == that.computingEngineType;
    }

    @Override
    public int hashCode() {
        int result = host != null ? host.hashCode() : 0;
        result = 31 * result + port;
        result = 31 * result + (computingEngineType != null ? computingEngineType.hashCode() : 0);
        return result;
    }
}
