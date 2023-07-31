package com.eggroll.core.pojo;
import com.eggroll.core.constant.StringConstants;
import lombok.Data;

import  java.sql.Timestamp;
import java.util.Arrays;
@Data
public class ErServerNode {
        private long id;
        private String name;
        private long clusterId;
        private ErEndpoint endpoint;
        private String nodeType;
        private String status;
        private Timestamp lastHeartBeat;
        private ErResource[] resources;

        public ErServerNode() {
            this.id = -1;
            this.name = StringConstants.EMPTY;
            this.clusterId = 0;
            this.endpoint = new ErEndpoint(StringConstants.EMPTY, -1);
            this.nodeType = StringConstants.EMPTY;
            this.status = StringConstants.EMPTY;
            this.lastHeartBeat = null;
            this.resources = new ErResource[0];
        }

        public ErServerNode(long id, String name, long clusterId, ErEndpoint endpoint,
                            String nodeType, String status, Timestamp lastHeartBeat,
                            ErResource[] resources) {
            this.id = id;
            this.name = name;
            this.clusterId = clusterId;
            this.endpoint = endpoint;
            this.nodeType = nodeType;
            this.status = status;
            this.lastHeartBeat = lastHeartBeat;
            this.resources = resources;
        }

        public ErServerNode(String nodeType, String status) {
            this.id = -1;
            this.name = StringConstants.EMPTY;
            this.clusterId = 0;
            this.endpoint = new ErEndpoint(StringConstants.EMPTY, -1);
            this.nodeType = nodeType;
            this.status = status;
            this.lastHeartBeat = null;
            this.resources = new ErResource[0];
        }

        @Override
        public String toString() {
            return "<ErServerNode(id=" + id + ", name=" + name +
                    ", clusterId=" + clusterId + ", endpoint=" + endpoint +
                    ", nodeType=" + nodeType + ", status=" + status +
                    ", lastHeartBeat=" + lastHeartBeat +
                    ", resources=" + Arrays.toString(resources) +
                    ") at " + Integer.toHexString(hashCode()) + ">";
        }
    }