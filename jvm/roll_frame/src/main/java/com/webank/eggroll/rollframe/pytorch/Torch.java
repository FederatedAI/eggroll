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


@Deprecated
public class Torch {
    /**
     * dot product between two array.
     */
    public static native float primaryDot(float[] v1, float[] v2);

    /**
     * matrix multiply vector
     */
    public static native double[] mm(long address, long size, double[] v);

    /**
     * matrix multiply local matrix
     */
    public static native double[] mm(long address, long size, double[] v, long rows, long cols);

    /**
     * interface of torch script, init a model and get model point.Maybe not necessary.
     */
    public static native long getTorchScript(String path);

    /**
     * torch script's interface
     * @param tensors: includes data and hpyer-parameters
     * @return double[] include data and hpyer-parameters,if only run Map, output exclude hpyer-parameters.
     */
    public static native double[] run(long ptr, TorchTensor[] tensors, double[] parameters);

//    public static native double[] run(long path, TorchTensor[] tensors);
}
