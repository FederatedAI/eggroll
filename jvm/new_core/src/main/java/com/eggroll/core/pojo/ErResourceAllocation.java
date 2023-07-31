package com.eggroll.core.pojo;

public class ErResourceAllocation  {
        private long serverNodeId;
        private String sessionId;
        private String operateType;
        private String status;
        private ErResource[] resources;

        public ErResourceAllocation(long serverNodeId, String sessionId, String operateType, String status, ErResource[] resources) {
            this.serverNodeId = serverNodeId;
            this.sessionId = sessionId;
            this.operateType = operateType;
            this.status = status;
            this.resources = resources;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (ErResource r : resources) {
                sb.append("[");
                sb.append(r.toString());
                sb.append("]");
            }
            return String.format("<ErResourceAllocation(serverNodeId=%d, sessionId=%s, operateType=%s, status=%s, resources=%s)>",
                    serverNodeId, sessionId, operateType, status, sb.toString());
        }
    }