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

#ifndef STORAGE_SERVICE_CXX_ROCKSDBSTORE_H
#define STORAGE_SERVICE_CXX_ROCKSDBSTORE_H

#include <string>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#include "SKVStore.h"

class RocksDBStore : public SKVStore {
public:
    RocksDBStore();
    RocksDBStore(const RocksDBStore& other);
    ~RocksDBStore();
    bool init(string& dbDir, StoreInfo& storeInfo);
    void put(const Operand* operand);
    long putAll(ServerReader<Operand>* reader);
    string_view putIfAbsent(const Operand* operand, string& result);
    string_view delOne(const Operand* operand, string& result);
    string_view get(const Operand* operand, string& result);
    void iterate(const Range* range, ServerWriter<Operand>* writer);
    bool destroy();
    long count();
    string toString();
private:
    void iterateAll();
    long PAYLOAD_THREASHOLD = 2L * 1024 * 1024;

    string _dbDir;
    StoreInfo storeInfo;
    /*
     * this pointer should never be transferred out of a class instance.
     * If there is a such requirement, consider wrapping it in a new class and us shared_ptr.
     */
    rocksdb::DB *_db;
    bool _isDeleteOnExit = false;
};


#endif //STORAGE_SERVICE_CXX_ROCKSDBSTORE_H
