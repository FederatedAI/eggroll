/*
 * Copyright 2019 The Eggroll Authors. All Rights Reserved.
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
 */

package com.webank.ai.eggroll.core.retry.impl.stop;

import com.google.common.base.Preconditions;
import com.webank.ai.eggroll.core.retry.AttemptContext;
import com.webank.ai.eggroll.core.retry.StopStrategy;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class StopAfterMaxDelayStrategy implements StopStrategy {
    private final long maxDelay;

    public StopAfterMaxDelayStrategy(long maxDelay) {
        Preconditions.checkArgument(maxDelay >= 0, "maxDelay must >= 0");
        this.maxDelay = maxDelay;
    }

    @Override
    public boolean shouldStop(AttemptContext<?> failedAttemptContext) {
        return failedAttemptContext.getElapsedTimeSinceFirstAttempt() >= maxDelay;
    }
}
