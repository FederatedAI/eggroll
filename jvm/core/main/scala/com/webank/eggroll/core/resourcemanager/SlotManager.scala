package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.constant.ResourceTypes
import com.webank.eggroll.core.meta.ErResource
import com.webank.eggroll.core.util.Logging

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

object SlotManager extends Logging{

   var allocatedSlots = mutable.Map[String,Slot]()

   var currentSlotId = new  AtomicLong(0)

   def   allocateSlot( sessionId:String,containerId: String,erResource:Array[ErResource]): Slot =synchronized {
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
      Slot(currentSlotId.addAndGet(1),erResource);
   }

//   private def checkResourceIsEnough(rtype:String ,count :Long ): Boolean ={
//      var resourceWrapper =  NodeResourceManager.getResourceWrapper(rtype)
//      if(resourceWrapper!=None){
//         var  left = resourceWrapper.get.total.get() - resourceWrapper.get.allocated.get()
//         if(count>= left)
//            return true;
//      }
//      false
//   }
//   private  def  allocateResource(rtype:String ,count:Long):Long ={
//      require(count>0)
//      var resourceWrapper =  NodeResourceManager.getResourceWrapper(rtype)
//      resourceWrapper.get.allocated.addAndGet(count);
//   }



   def  freeSlot(slotId:String):Boolean = synchronized{

     false
   }


   def main(args: Array[String]): Unit = {
      // allocateSlot("","",erResource=Array())
      allocateSlot(sessionId = "",containerId="",erResource = Array(ErResource(resourceType = ResourceTypes.PHYSICAL_MEMORY,total = 1000)
                                 ,ErResource(resourceType = ResourceTypes.VCPU_CORE,total =  7),ErResource(resourceType = ResourceTypes.VCPU_CORE,total=9)));


   }

}
case class Slot(slotId:Long  ,
                resources:Array[ErResource]=Array())


