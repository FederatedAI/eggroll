package com.webank.eggroll.nodemanager.service;

import org.fedai.eggroll.core.config.Dict;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.webank.eggroll.guice.module.NodeModule;
import com.webank.eggroll.nodemanager.pojo.ResourceWrapper;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class NodeResourceManagerTest {

    private static NodeResourceManager nodeResourceManager;

    @BeforeClass
    public static void before() {
        Injector injector = Guice.createInjector(new NodeModule());
        nodeResourceManager = injector.getInstance(NodeResourceManager.class);
    }

    @Test
    public void testGetResourceWrapper() {
        ResourceWrapper resourceWrapper = nodeResourceManager.getResourceWrapper(Dict.VCPU_CORE);
        Assert.assertTrue(resourceWrapper.getTotal().get() > 0);
    }

    @Test
    public void testCheckResourceIsEnough() {
        Boolean isEnough = nodeResourceManager.checkResourceIsEnough(Dict.VCPU_CORE, 2L);
        Assert.assertTrue(isEnough);
    }

    @Test
    public void testFreeResource() {
        Long resourceSize = 2L;
        Long beforeValue = nodeResourceManager.freeResource(Dict.VCPU_CORE, resourceSize);
        long afterValue = nodeResourceManager.getResourceWrapper(Dict.VCPU_CORE).getAllocated().get();
        Assert.assertTrue(beforeValue - afterValue == resourceSize);
    }

    @Test
    public void testAllocateResource() {
        Long resourceSize = 2L;
        Long beforeValue = nodeResourceManager.allocateResource(Dict.VCPU_CORE, resourceSize);
        long afterValue = nodeResourceManager.getResourceWrapper(Dict.VCPU_CORE).getAllocated().get();
        Assert.assertTrue(afterValue - beforeValue == resourceSize);
    }

    @Test
    public void testStart() {

    }

    @Test
    public void testGetPhysicalMemorySize() {
        Long physicalMemorySize = nodeResourceManager.getPhysicalMemorySize();
        Assert.assertTrue(physicalMemorySize > 0);
    }

    @Test
    public void testGetGpuSize() {
        Assert.assertSame(0, nodeResourceManager.getGpuSize());
    }

    @Test
    public void testGetAvailablePhysicalMemorySize() {
        Long availablePhysicalMemorySize = nodeResourceManager.getAvailablePhysicalMemorySize();
        Assert.assertTrue(availablePhysicalMemorySize > 0);
    }

    @Test
    public void testGetAvailableProcessors() {
        int availableProcessors = nodeResourceManager.getAvailableProcessors();
        Assert.assertTrue(availableProcessors > 0);
    }

    @Test
    public void testCountMemoryResource() {
        try {
            nodeResourceManager.countMemoryResource();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
        Assert.assertTrue(true);
    }

    @Test
    public void testCountCpuResource() {
        try {
            nodeResourceManager.countCpuResource();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
        Assert.assertTrue(true);
    }

    @Test
    public void testCountGpuResource() {
        try {
            nodeResourceManager.countGpuResource();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
        Assert.assertTrue(true);
    }

}