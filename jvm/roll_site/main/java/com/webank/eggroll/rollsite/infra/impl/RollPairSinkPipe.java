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

package com.webank.eggroll.rollsite.infra.impl;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.ErRollSiteHeader;
import com.webank.eggroll.core.meta.TransferModelPbMessageSerdes;
import com.webank.eggroll.core.transfer.Transfer.RollSiteHeader;
import com.webank.eggroll.rollsite.RollSiteUtil;
import com.webank.eggroll.rollsite.infra.JobStatus;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import scala.collection.immutable.Map.Map1;

public class RollPairSinkPipe extends BasePipe {

  private ErRollSiteHeader erRollSiteHeader;
  private RollSiteUtil rollSiteUtil;
  private String erSessionId;
  private String rollSiteSessionId;

  public RollPairSinkPipe(ErRollSiteHeader erRollSiteHeader) {
    this.erRollSiteHeader = erRollSiteHeader;
    this.rollSiteSessionId = erRollSiteHeader.rollSiteSessionId();

    try {
      while (!JobStatus.isJobIdToSessionRegistered(rollSiteSessionId)) {
          Thread.sleep(500L);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    this.erSessionId = JobStatus.getErSessionId(rollSiteSessionId);
    this.rollSiteUtil = new RollSiteUtil(erSessionId, erRollSiteHeader, new Map1<>("", ""));
  }

  public RollPairSinkPipe(String erRollSiteHeaderString) throws InvalidProtocolBufferException {
    this(TransferModelPbMessageSerdes.ErRollSiteHeaderFromPbMessage(
        RollSiteHeader.parseFrom(erRollSiteHeaderString.getBytes(StandardCharsets.ISO_8859_1))).fromProto());
  }

  @Override
  public Object read(long timeout, TimeUnit unit) {
    throw new UnsupportedOperationException("Read operation not support in RollPairSinkPipe");
  }

  @Override
  public void write(Object o) {
    if (o instanceof ByteString) {
      this.rollSiteUtil.putBatch((ByteString) o);
    } else {
      throw new IllegalArgumentException("Argument must be a ByteString instance");
    }
  }


}
