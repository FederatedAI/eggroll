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

#ifndef STORAGE_SERVICE_CXX_LMDBSTORE_H
#define STORAGE_SERVICE_CXX_LMDBSTORE_H

#include <algorithm>
#include <array>
#include <memory>
#include <iostream>
#include <string>
#include <sstream>

#include <sys/stat.h>
#include <sys/types.h>

#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/utility/string_view.hpp>

#include <grpcpp/grpcpp.h>
#include <grpcpp/server_context.h>
#include <glog/logging.h>

#include "lmdb++.h"
#include "properties.h"
#include "storage.pb.h"
#include "storage.grpc.pb.h"

#include "StoreInfo.h"
#include "ExceptionHandler.h"
#include "third_party/lmdb-safe/lmdb-safe.hh"

using std::string;
using com::webank::ai::eggroll::api::storage::Operand;
using com::webank::ai::eggroll::api::storage::Range;
using grpc::ServerReader;
using grpc::ServerWriter;
using eggroll::handle_eptr;

class LMDBStore {
public:
    LMDBStore();
    LMDBStore(const LMDBStore& other);
    ~LMDBStore();
    bool init(string dataDir, StoreInfo& storeInfo);
    void put(const Operand* operand);
    long putAll(ServerReader<Operand>* reader);
    string_view putIfAbsent(const Operand* operand);
    string_view delOne(const Operand* operand);
    string_view get(const Operand* operand);
    void iterate(const Range* range, ServerWriter<Operand>* writer);
    bool destroy();
    long count();
    string toString();
private:
    lmdb::txn createTxn(bool isWrite);
    lmdb::dbi createDbi(lmdb::txn txn);
    lmdb::cursor createCursor(lmdb::txn txn, lmdb::dbi dbi);
    void iterateAll();

    string _dbDir;
    StoreInfo storeInfo;

    std::shared_ptr<MDBEnv> _env;
    MDBDbi _dbi;

    long PAYLOAD_THREASHOLD = 2L * 1024 * 1024;
};

#endif //STORAGE_SERVICE_CXX_LMDBSTORE_H
