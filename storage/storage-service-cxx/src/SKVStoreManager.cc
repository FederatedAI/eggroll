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

#include "SKVStoreManager.h"

using std::cout;
using std::endl;

std::shared_ptr<SKVStore> SKVStoreManager::getStore(ServerContext *context, const string& dataDir) {
    struct StoreValue {
        std::weak_ptr<SKVStore> wp;
    };

    static std::mutex mtx;
    static std::map<string, StoreValue> envs;

    shared_ptr<SKVStore> store = NULL;
    std::exception_ptr eptr;
    try {
        std::multimap <grpc::string_ref, grpc::string_ref> metadata = context->client_metadata();
        bool initResult = false;
        StoreInfo storeInfo(context);
        string dbDir(StorageUtils::generateDbDir(dataDir, storeInfo));

        if (boost::iequals(storeInfo.getStoreType(), LEVEL_DB)) {
            bool needCreate = false;

            std::lock_guard<std::mutex> l(mtx);
            auto iter = envs.find(dbDir);

            if (iter != envs.end()) {
                cout << "env found for " << dbDir << endl;
                store = iter->second.wp.lock();
                if (!store) {
                    cout << "env found but invalid" << endl;
                    needCreate = true;
                    envs.erase(iter);
                }
            } else {
                cout << "env not found for " << dbDir << endl;
                needCreate = true;
            }

            if (needCreate) {
                store = std::make_shared<RocksDBStore>();
                initResult = store->init(dbDir, storeInfo);
                envs[dbDir] = {store};
            }
        } else {
            store = std::make_shared<LMDBStore>();
            initResult = store->init(dbDir, storeInfo);
        }

        // todo: add exception if result is false
        if (!initResult) {
            std::string errorMsg = "Unable to init SKVStore. please check error log.";
            throw std::runtime_error(errorMsg);
        }
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, "SKVServicer::getStore");

    return store;
}