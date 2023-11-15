package org.fedai.eggroll.core.containers.container;


import java.util.EnumSet;

public enum ContainerStatus {
    Pending,
    Started,
    Failed,
    Poison,
    Success,
    Exception;
}
