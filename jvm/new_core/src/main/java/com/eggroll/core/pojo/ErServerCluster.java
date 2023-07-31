package com.eggroll.core.pojo;

import com.eggroll.core.constant.StringConstants;

import java.util.Arrays;

public class ErServerCluster {
        private long id;
        private String name;
        private ErServerNode[] serverNodes;
        private String tag;

        public ErServerCluster() {
            this.id = -1;
            this.name = StringConstants.EMPTY;
            this.serverNodes = new ErServerNode[0];
            this.tag = StringConstants.EMPTY;
        }

        public ErServerCluster(long id, ErServerNode[] serverNodes, String tag) {
            this.id = id;
            this.name = StringConstants.EMPTY;
            this.serverNodes = serverNodes;
            this.tag = tag;
        }

        @Override
        public String toString() {
            return "<ErServerCluster(id=" + id + ", name=" + name +
                    ", serverNodes=" + Arrays.toString(serverNodes) + ", tag=" + tag +
                    ") at " + Integer.toHexString(hashCode()) + ">";
        }
    }