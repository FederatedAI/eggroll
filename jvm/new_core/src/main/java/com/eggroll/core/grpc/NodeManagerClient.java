/*
 * Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.eggroll.core.grpc;

import com.eggroll.core.containers.meta.KillContainersResponse;
import com.eggroll.core.containers.meta.StartContainersResponse;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.eggroll.core.grpc.CommandUri.*;


public class NodeManagerClient {
  Logger logger = LoggerFactory.getLogger(NodeManagerClient.class);

  CommandClient  commandClient;
  ErEndpoint nodeManagerEndpoint;
  public  NodeManagerClient(ErEndpoint nodeManagerEndpoint){
    logger.info("create new  nodemanager client {}",nodeManagerEndpoint);
    if (nodeManagerEndpoint == null )
      throw new IllegalArgumentException("failed to create NodeManagerClient for endpoint: " + nodeManagerEndpoint);
    this.nodeManagerEndpoint = nodeManagerEndpoint;
    commandClient= new CommandClient();
  }


//  public static final String startContainers = "v1/node_manager/processor/startContainers";
//  public static final String stopContainers = "v1/node_manager/processor/stopContainers";
//  public static final String killContainers = "v1/node_manager/processor/killContainers";
//  public static final String nodeHeartbeat = "v1/node_manager/processor/heartbeat";

//  public ErProcessor heartbeat(ErProcessor processor){
//    byte[] responseData = commandClient.call(nodeManagerEndpoint, null,processor.serialize());
//    ErProcessor response = new ErProcessor();
//    response.deserialize(responseData);
//    return response;
//  }

  public ErSessionMeta  startContainers(Context context, ErSessionMeta sessionMeta) {
    byte[] responseData = commandClient.call(context,nodeManagerEndpoint, startContainers,sessionMeta.serialize());
    ErSessionMeta response = new ErSessionMeta();
    response.deserialize(responseData);
    return response;
  }

  public ErSessionMeta  stopContainers(Context context,ErSessionMeta sessionMeta) {
    byte[] responseData = commandClient.call(context,nodeManagerEndpoint, stopContainers,sessionMeta.serialize());
    ErSessionMeta response = new ErSessionMeta();
    response.deserialize(responseData);
    return response;
  }


  public StartContainersResponse startJobContainers(Context context,StartContainersRequest startContainersRequest) {
    byte[] responseData = commandClient.call(context,nodeManagerEndpoint, startJobContainers,startContainersRequest.serialize());
    StartContainersResponse response = new StartContainersResponse();
    response.deserialize(responseData);
    return response;
  }


  public ErSessionMeta  killContainers(Context context,ErSessionMeta sessionMeta) {
    byte[] responseData = commandClient.call(context,nodeManagerEndpoint, killContainers,sessionMeta.serialize());
    ErSessionMeta response = new ErSessionMeta();
    response.deserialize(responseData);
    return response;
  }

  public KillContainersResponse killJobContainers(Context context,KillContainersRequest killContainersRequest){
    byte[] responseData = commandClient.call(context,nodeManagerEndpoint, killContainers, killContainersRequest.serialize());
    KillContainersResponse response = new KillContainersResponse();
    response.deserialize(responseData);
    return response;
  }

  public ErProcessor checkNodeProcess( Context context,ErProcessor processor){
    byte[] responseData = commandClient.call(context,nodeManagerEndpoint, checkNodeProcess,processor.serialize());
    ErProcessor response = new ErProcessor();
    response.deserialize(responseData);
    return response;
  }

  //  def checkNodeProcess(processor: ErProcessor): ErProcessor =
//  commandClient.call[ErProcessor](ResouceCommands.checkNodeProcess, processor)











//
//  def startJobContainers(startContainersRequest: StartContainersRequest): StartContainersResponse = {
//    commandClient.call(ContainerCommands.startJobContainers, startContainersRequest)
//  }
//
//  def stopJobContainers(stopContainersRequest: StopContainersRequest): StopContainersResponse = {
//    commandClient.call(ContainerCommands.stopJobContainers, stopContainersRequest)
//  }
//
//  def killJobContainers(killContainersRequest: KillContainersRequest): KillContainersResponse =
//          commandClient.call(ContainerCommands.killJobContainers, killContainersRequest)
//
//  def allocateResource(srcAllocate: ErResourceAllocation): ErResourceAllocation =
//  commandClient.call[ErResourceAllocation](ResouceCommands.resourceAllocation, srcAllocate)
//
//  def queryNodeResource(erServerNode: ErServerNode): ErServerNode =
//  commandClient.call[ErServerNode](ResouceCommands.queryNodeResource, erServerNode)
//
//  def downloadContainers(downloadContainersRequest: DownloadContainersRequest): DownloadContainersResponse =
//          commandClient.call(ContainerCommands.downloadContainers, downloadContainersRequest)
//
//  def checkNodeProcess(processor: ErProcessor): ErProcessor =
//  commandClient.call[ErProcessor](ResouceCommands.checkNodeProcess, processor)









}
