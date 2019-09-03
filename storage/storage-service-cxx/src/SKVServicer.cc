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

#include "SKVServicer.h"

using std::cout;
using std::endl;
using std::string;

SKVServicer::SKVServicer() : SKVServicer::SKVServicer("/tmp") {}

SKVServicer::SKVServicer(std::string dataDir) {
    this->dataDir = dataDir;
}

SKVServicer::~SKVServicer() {

}

Status defaultStatus = Status::OK;

// put an entry to table
Status SKVServicer::put(ServerContext *context, const Operand *request, Empty *response) {
    std::exception_ptr eptr;
    try {
        cout << "put request" << endl;
        LOG(INFO) << "put request" << endl;
        shared_ptr<SKVStore> skvStore = SKVStoreManager::getStore(context, dataDir);

        skvStore->put(request);

        LOG(INFO) << "put finished" << endl;
        cout << "put finished" << endl;
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, "SKVServicer::put");

    return Status::OK;
}

// put an entry to table if absent
Status SKVServicer::putIfAbsent(ServerContext *context, const Operand *request, Operand *response) {
    std::exception_ptr eptr;
    try {
        LOG(INFO) << "putIfAbsent request" << endl;
        cout << "putIfAbsent request" << endl;
        shared_ptr<SKVStore> skvStore = SKVStoreManager::getStore(context, dataDir);

        string result;
        string_view oldValue = skvStore->putIfAbsent(request, result);

        response->set_key(request->key());
        response->set_value(oldValue.data(), oldValue.size());

        LOG(INFO) << "putIfAbsent finished" << endl;
        cout << "putIfAbsent finished" << endl;
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, "SKVServicer::putIfAbsent");

    return Status::OK;
}

// put entries to table (entries will be streaming in)
Status SKVServicer::putAll(ServerContext *context, ServerReader<Operand> *reader, Empty *response) {
    std::exception_ptr eptr;
    try {
        LOG(INFO) << "putAll request" << endl;
        cout << "putAll request" << endl;
        shared_ptr<SKVStore> skvStore = SKVStoreManager::getStore(context, dataDir);

        skvStore->putAll(reader);

        LOG(INFO) << "putAllFinished" << endl;
        cout << "putAllFinished" << endl;
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, "SKVServicer::putAll");

    return Status::OK;
}

// delete an entry from table
Status SKVServicer::delOne(ServerContext *context, const Operand *request, Operand *response) {
    std::exception_ptr eptr;
    try {
        LOG(INFO) << "delOne request" << endl;
        cout << "delOne request" << endl;
        shared_ptr<SKVStore> skvStore = SKVStoreManager::getStore(context, dataDir);

        string result;
        string_view oldValue = skvStore->delOne(request, result);

        response->set_key(request->key());
        response->set_value(oldValue.data(), oldValue.size());

        LOG(INFO) << "delOne finished" << endl;
        cout << "delOne finished" << endl;
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, "SKVServicer::delOne");

    return Status::OK;
}

// get an entry from table
Status SKVServicer::get(ServerContext *context, const Operand *request, Operand *response) {
    std::exception_ptr eptr;
    try {
        LOG(INFO) << "get request" << endl;
        cout << "get request" << endl;

        shared_ptr<SKVStore> skvStore = SKVStoreManager::getStore(context, dataDir);

        string value;
        string_view result = skvStore->get(request, value);

        response->set_key(request->key());
        response->set_value(result.data(), result.size());

        LOG(INFO) << "get finished" << endl;
        cout << "get finished" << ", value: " << value << endl;
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, "SKVServicer::get");

    return Status::OK;
}

// iterate through a table. Response entries are ordered
Status SKVServicer::iterate(ServerContext *context, const Range *request, ServerWriter<Operand> *writer) {
    std::exception_ptr eptr;
    try {
        LOG(INFO) << "iterate request" << endl;
        cout << "iterate request" << endl;
        shared_ptr<SKVStore> skvStore = SKVStoreManager::getStore(context, dataDir);

        skvStore->iterate(request, writer);

        LOG(INFO) << "iterate finished" << endl;
        cout << "iterate finished" << endl;
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, "SKVServicer::iterate");

    return Status::OK;
}

// destroy a table
Status SKVServicer::destroy(ServerContext *context, const Empty *request, Empty *response) {
    std::exception_ptr eptr;
    try {
        LOG(INFO) << "destroy request" << endl;
        cout << "destroy request" << endl;
        shared_ptr<SKVStore> skvStore = SKVStoreManager::getStore(context, dataDir);

        skvStore->destroy();

        LOG(INFO) << "destroy finished" << endl;
        cout << "destroy finished" << endl;
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, "SKVServicer::destroy");

    return Status::OK;
}

Status SKVServicer::destroyAll(ServerContext *context, const Empty *request, Empty *response) {
    std::exception_ptr eptr;
    try {
        LOG(INFO) << "destroyAll request" << endl;
        cout << "destroyAll request" << endl;
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, "SKVServicer::destroyAll");

    return Status::OK;
}

// count record amount of a table
Status SKVServicer::count(ServerContext *context, const Empty *request, Count *response) {
    std::exception_ptr eptr;
    try {
        LOG(INFO) << "count request" << endl;
        cout << "count request" << endl;
        shared_ptr<SKVStore> skvStore = SKVStoreManager::getStore(context, dataDir);

        response->set_value(skvStore->count());

        LOG(INFO) << "count finished" << endl;
        cout << "count finished" << endl;
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, "SKVServicer::count");

    return Status::OK;
}

Status SKVServicer::createIfAbsent(ServerContext *context, const CreateTableInfo *request, CreateTableInfo *response) {
    std::exception_ptr eptr;
    try {
        LOG(INFO) << "createIfAbsent request" << endl;
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, "SKVServicer::createIfAbsent");

    return Status::OK;
}

void SKVServicer::sayHello() {
    LOG(INFO) << "saying hello" << endl;
}

shared_ptr<SKVStore> SKVServicer::getStore(ServerContext *context) {
/*    shared_ptr<SKVStore> store = NULL;
    std::exception_ptr eptr;
    try {
        std::multimap <grpc::string_ref, grpc::string_ref> metadata = context->client_metadata();

        StoreInfo storeInfo(context);

        if (boost::iequals(storeInfo.getStoreType(), LEVEL_DB)) {
            store = std::make_shared<RocksDBStore>();
        } else {
            store = std::make_shared<LMDBStore>();
        }

        bool result = store->init(dataDir, storeInfo);

        // todo: add exception if result is false
        if (!result) {
            std::string errorMsg = "Unable to init SKVStore. please check error log.";
            throw std::runtime_error(errorMsg);
        }
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, "SKVServicer::getStore");

    return store;*/

    return SKVStoreManager::getStore(context, dataDir);
}