package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.meta.ErProcessor

trait ProcessorEventCallback {
   def  callback(event: ProcessorEvent)

}

case class ProcessorEvent(eventType:String,erProcessor: ErProcessor)
