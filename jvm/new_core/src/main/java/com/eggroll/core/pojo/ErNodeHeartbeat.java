package com.eggroll.core.pojo;

public class ErNodeHeartbeat {
        private long id;
        private ErServerNode node;

        public ErNodeHeartbeat() {
            this.id = -1;
            this.node = null;
        }

        public ErNodeHeartbeat(long id, ErServerNode node) {
            this.id = id;
            this.node = node;
        }

        @Override
        public String toString() {
            return "<ErNodeHeartbeat(id=" + id + ", node=" + node +
                    ") at " + Integer.toHexString(hashCode()) + ">";
        }
    }
