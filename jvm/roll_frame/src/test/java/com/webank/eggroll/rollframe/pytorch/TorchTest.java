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
 */

package com.webank.eggroll.rollframe.pytorch;

import com.webank.eggroll.rollframe.TestAssets;
import org.junit.Before;
import org.junit.Test;
import junit.framework.TestCase;

public class TorchTest {

    @Before
    public void loadLibrary() {
        System.loadLibrary("roll_frame_torch");
    }

    @Test
    public void testPrimaryDot() {
        float[] input1 = {2.0f, 2.0f, 1.0f, 2.0f, 2.0f};
        float[] input2 = {-1.0f, 1.0f, 1.0f, -1.0f, 1.0f};
        float res = Torch.primaryDot(input1, input2);
        TestCase.assertEquals(res, 1.0, TestAssets.DELTA());
    }
}
