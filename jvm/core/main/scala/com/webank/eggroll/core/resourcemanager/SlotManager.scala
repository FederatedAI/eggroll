package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.constant.ResourceTypes
import com.webank.eggroll.core.meta.ErResource
import com.webank.eggroll.core.util.Logging
import org.apache.commons.lang3.StringUtils

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

object SlotManager extends Logging{

   var allocatedSlots = mutable.Map[Long,Slot]()

   var sessionSlot = mutable.Map[String,mutable.Set[Long]]()

   var currentSlotId = new  AtomicLong(0)

   def   allocateSlot( sessionId:String,containerId: String,erResource:Array[ErResource]): Slot =synchronized {
      require(StringUtils.isNotEmpty(sessionId))
      var prepareToAllocateResource = mutable.Map[String,Long]();
      erResource.groupBy(n=>{
         n.resourceType
      }).foreach(e=>{
         prepareToAllocateResource.put(e._1, e._2.map(n=>{n.total}).sum)
      })
      var  enough :Boolean = true;
      prepareToAllocateResource.foreach((e)=>{
         if(!NodeResourceManager.checkResourceIsEnough(e._1,e._2)){
            enough = false;
         }
      })
      erResource.foreach(e=>{
         var alreadyAllocated = NodeResourceManager.allocateResource(e.resourceType,e.total)
         logInfo(s"sessionId ${sessionId} containerId ${containerId} allocate ${e.resourceType} ${e.total} , already allocate ${alreadyAllocated}")
      })
      var slot = Slot(slotId=currentSlotId.addAndGet(1),
         sessionId = sessionId,
         containerId = containerId,
         resources=erResource);
      allocatedSlots.put(slot.slotId,slot)
      if(sessionSlot.contains(sessionId)){
         sessionSlot.get(sessionId).get.+=(slot.slotId)
      }
      slot
   }

   def  freeSlotBySlotId(slotId:Long):Unit = synchronized{
      if(allocatedSlots.contains(slotId)){
         allocatedSlots.get(slotId).get.resources.foreach(r=>{
            NodeResourceManager.freeResource(r.resourceType,r.total)
         })
      }
   }

   def  freeSlotBySessionId(sessionId: String ): Unit={
      if(sessionSlot.contains(sessionId)){
         sessionSlot.get(sessionId).get.foreach(slotId=>{
            freeSlotBySlotId(slotId)
         })
      }
   }


   def main(args: Array[String]): Unit = {
      // allocateSlot("","",erResource=Array())
      allocateSlot(sessionId = "",containerId="",erResource = Array(ErResource(resourceType = ResourceTypes.PHYSICAL_MEMORY,total = 1000)
                                 ,ErResource(resourceType = ResourceTypes.VCPU_CORE,total =  7),ErResource(resourceType = ResourceTypes.VCPU_CORE,total=9)));


   }

}
case class Slot(slotId:Long  ,
                sessionId: String ,
                containerId: String,
                resources:Array[ErResource]=Array())


