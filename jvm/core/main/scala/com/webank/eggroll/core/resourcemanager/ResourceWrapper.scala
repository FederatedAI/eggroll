package com.webank.eggroll.core.resourcemanager

import java.util.concurrent.atomic.AtomicLong

case class ResourceWrapper(resourceType:String,
                     // allocate:AtomicLong=new AtomicLong(0),
                      total:AtomicLong=new AtomicLong(0),
                      used : AtomicLong=new AtomicLong(0),
                      allocated:AtomicLong=new AtomicLong(0)
                     ) ;