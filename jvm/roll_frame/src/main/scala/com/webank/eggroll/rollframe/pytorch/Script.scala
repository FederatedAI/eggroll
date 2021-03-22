package com.webank.eggroll.rollframe.pytorch

import com.webank.eggroll.format.{FrameBatch, FrameSchema, FrameUtils}
import com.webank.eggroll.util.SchemaUtil

object Script {
  // TODO: output tensor share the same memory
  def runTorchMap(path:String, fb: FrameBatch, parameters: Array[Double]): FrameBatch = {
    val tensor = new TorchTensor
    tensor.setAddress(fb.rootVectors(0).getDataBufferAddress)
    tensor.setSize(fb.rowCount)
    val ptr = Torch.getTorchScript(path)
    val partitionResult = Torch.run(ptr, Array(tensor), parameters)
    val rootSchema = new FrameSchema(SchemaUtil.oneDoubleFieldSchema)
    val outFb = new FrameBatch(rootSchema, partitionResult.size)
    FrameUtils.copyMemory(outFb.rootVectors(0), partitionResult)
    outFb
  }

  def runTorchMerge(path:String, fbs: Iterator[FrameBatch], parameters: Array[Double]): FrameBatch = {
    var tensors:Array[TorchTensor] = Array()
    fbs.foreach{fb =>
      val tensor = new TorchTensor
      tensor.setAddress(fb.rootVectors(0).getDataBufferAddress)
      tensor.setSize(fb.rowCount)
      tensors = tensors ++ Array(tensor)
    }

    val ptr = Torch.getTorchScript(path)
    val partitionResult = Torch.run(ptr, tensors, parameters)
    val rootSchema = new FrameSchema(SchemaUtil.oneDoubleFieldSchema)
    val outFb = new FrameBatch(rootSchema, partitionResult.size)
    FrameUtils.copyMemory(outFb.rootVectors(0), partitionResult)
    outFb
  }
}
