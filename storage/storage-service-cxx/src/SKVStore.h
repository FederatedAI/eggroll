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

#ifndef STORAGE_SERVICE_CXX_SKVSTORE_H
#define STORAGE_SERVICE_CXX_SKVSTORE_H

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

#include "properties.h"
#include "storage.pb.h"
#include "storage.grpc.pb.h"

#include "StoreInfo.h"
#include "ExceptionHandler.h"
#include "third_party/lmdb-safe/lmdb-safe.hh"

using std::string;
using std::shared_ptr;
using grpc::ServerReader;
using grpc::ServerWriter;
using com::webank::ai::eggroll::api::storage::Operand;
using com::webank::ai::eggroll::api::storage::Range;

using eggroll::handle_eptr;

class SKVStore {
public:
    SKVStore() {}
    SKVStore(const SKVStore& other) {}
    virtual ~SKVStore() {}
    virtual bool init(string& dbDir, StoreInfo& storeInfo) = 0;
    virtual void put(const Operand* operand) = 0;
    virtual long putAll(ServerReader<Operand>* reader) = 0;
    virtual string_view putIfAbsent(const Operand* operand, string& result) = 0;
    virtual string_view delOne(const Operand* operand, string& result) = 0;
    virtual string_view get(const Operand* operand, string& result) = 0;
    virtual void iterate(const Range* range, ServerWriter<Operand>* writer) = 0;
    virtual bool destroy() = 0;
    virtual long count() = 0;
    virtual string toString() = 0;
};

#endif //STORAGE_SERVICE_CXX_SKVSTORE_H