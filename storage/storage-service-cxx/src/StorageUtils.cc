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

#include "StorageUtils.h"

using std::string;
using std::cout;
using std::endl;
using eggroll::handle_eptr;

string StorageUtils::generateDbDir(const string& dataDir, StoreInfo& storeInfo) {
    static string delimiter = "/";
    std::stringstream ss;
    string result;
    std::exception_ptr eptr;
    try {
        cout << storeInfo.toString() << endl;

        string storeType(storeInfo.getStoreType().data(), storeInfo.getStoreType().size());
        boost::algorithm::to_lower(storeType);
        ss << dataDir
           << delimiter << storeType
           << delimiter << storeInfo.getNameSpace()
           << delimiter << storeInfo.getTableName()
           << delimiter << storeInfo.getFragment() << delimiter;

        ss >> result;
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, "StorageUtils");

    return result;
}
