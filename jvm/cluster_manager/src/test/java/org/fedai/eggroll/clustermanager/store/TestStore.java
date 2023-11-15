package org.fedai.eggroll.clustermanager.store;

import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.grpc.ClusterManagerClient;
import org.fedai.eggroll.core.pojo.ErEndpoint;
import org.fedai.eggroll.core.pojo.ErStore;
import org.fedai.eggroll.core.pojo.ErStoreLocator;
import org.junit.Test;

public class TestStore {

    ErEndpoint endpoint = new ErEndpoint("localhost:4670");

    @Test
    public void testGetOrCreateStore() {
        ErStore erStore = new ErStore();
        ErStoreLocator erStoreLocator = new ErStoreLocator();
        erStoreLocator.setStoreType("ROLLPAIR_CACHE");
        erStoreLocator.setNamespace("np");
        erStoreLocator.setName("nn");
        erStoreLocator.setPath("/data/a11");
        erStoreLocator.setTotalPartitions(8);
        erStoreLocator.setPartitioner("aaa0");
        erStoreLocator.setSerdes("aaa");
        erStore.setStoreLocator(erStoreLocator);
        ClusterManagerClient clusterManagerClient = new ClusterManagerClient(endpoint);
        ErStore orCreateStore = clusterManagerClient.getOrCreateStore(new Context(), erStore);
        System.out.println("orCreateStore = " + orCreateStore);

    }
}
