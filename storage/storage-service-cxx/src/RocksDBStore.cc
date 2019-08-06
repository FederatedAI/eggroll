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

#include "RocksDBStore.h"

using std::cout;
using std::endl;

using rocksdb::Iterator;
using rocksdb::DB;
using rocksdb::Options;
using rocksdb::ReadOptions;
using rocksdb::Status;
using rocksdb::WriteBatch;
using rocksdb::WriteOptions;

RocksDBStore::RocksDBStore() {
    this->_db = NULL;
}

RocksDBStore::RocksDBStore(const RocksDBStore& other) {
    this->_dbDir = other._dbDir;
    this->storeInfo = other.storeInfo;
}

RocksDBStore::~RocksDBStore() {
    if (NULL != this->_db) {
        _db->Close();

        if (_isDeleteOnExit) {
            rocksdb::DestroyDB(_dbDir, rocksdb::Options());
        }
        delete this->_db;
        this->_db = NULL;
    }
}

void printStatus(Status &s, const string& prefix) {
    cout << prefix << ": code: " << s.code() << ", subcode: " << s.subcode() << endl;
}

bool RocksDBStore::init(string& dbDir, StoreInfo& storeInfo) {
    string delimiter = "/";
    bool result = true;
    std::exception_ptr eptr;
    try {
        cout << "[RocksDBStore::init]" << endl;
        LOG(INFO) << "[RocksDBStore::init]" << endl;
        if (NULL != this->_db) {
            return false;
        }
        this->_dbDir = dbDir;

        boost::filesystem::path dst = this->_dbDir;
        boost::filesystem::create_directories(dst);

        Options options;
        options.IncreaseParallelism();
        options.OptimizeLevelStyleCompaction();
        options.create_if_missing = true;

        Status s = DB::Open(options, _dbDir, &_db);
        printStatus(s, "init");
        assert(s.ok());
    } catch (...) {
        eptr = std::current_exception();
        result = false;
    }
    handle_eptr(eptr, __FILE__, __LINE__, this->toString());

    return result;
}

void RocksDBStore::put(const Operand *operand) {
    LOG(INFO) << "[RocksDBStore::put] dbDir: " << _dbDir << endl;
    cout << "[RocksDBStore::put] dbDir: " << _dbDir << endl;

    std::exception_ptr eptr;
    try {
        cout << "key: " << operand->key() << ", value: " << operand->value() << endl;
        Status s = _db->Put(WriteOptions(), operand->key(), operand->value());
        cout << "[RocksDBStore::put] finished, isok: " << s.ok() << endl;
        assert(s.ok());
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, this->toString());
}

long RocksDBStore::putAll(ServerReader<Operand> *reader) {
    LOG(INFO) << "[RocksDBStore::putAll] dbDir: " << _dbDir << endl;

    long i = 0;
    std::exception_ptr eptr;
    try {
        Operand operand;

        long count = 0;
        long countInterval = 100000;
        long countRemaining = countInterval;
        WriteBatch batch;
        while (reader->Read(&operand)) {
            batch.Put(operand.key(), operand.value());
            ++count;
        }

        _db->Write(WriteOptions(), &batch);
        cout << "[RocksDBStore::putAll] dbDir: " << _dbDir << ", total putAll: " << count << endl;
        LOG(INFO) << "[RocksDBStore::putAll] dbDir: " << _dbDir << ", total putAll: " << count << endl;
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, this->toString());
    return i;
}

string_view RocksDBStore::putIfAbsent(const Operand *operand, string& result) {
    LOG(INFO) << "[RocksDBStore::putIfAbsent] dbDir: " << _dbDir << endl;

    string_view resultView;
    std::exception_ptr eptr;
    try {
        Status s = _db->Get(ReadOptions(), operand->key(), &result);
        if (s.IsNotFound()) {
            _db->Put(WriteOptions(), operand->key(), operand->value());
            result = operand->value();
        }
        resultView = result;
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, this->toString());

    return resultView;
}

string_view RocksDBStore::delOne(const Operand *operand, string& result) {
    LOG(INFO) << "[RocksDBStore::delOne] dbDir: " << _dbDir << endl;

    string_view oldValue;

    std::exception_ptr eptr;
    try {
        Status s = _db->Get(ReadOptions(), operand->key(), &result);
        if (!s.IsNotFound()) {
            _db->Delete(WriteOptions(), operand->key());
            result = "";
        }
        oldValue = result;
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, this->toString());

    return oldValue;
}

bool RocksDBStore::destroy() {
    LOG(INFO) << "[RocksDBStore::destroy] dbDir: " << _dbDir << endl;
    bool result = false;
    std::exception_ptr eptr;
    size_t n;
    try {
        _isDeleteOnExit = true;
        result = true;
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, this->toString());

    return result;
}

long RocksDBStore::count() {
    LOG(INFO) << "[RocksDBStore::count] dbDir: " << _dbDir << endl;
    long result = 0;

    std::exception_ptr eptr;
    try {
        Iterator *it = _db->NewIterator(ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            ++result;
        }

        delete it;
        cout << "count: " << result << endl;
        LOG(INFO) << "count: " << result << endl;
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, this->toString());

    return result;
}

string_view RocksDBStore::get(const Operand *operand, string& result) {
    LOG(INFO) << "[RocksDBStore::get] dbDir: " << _dbDir << endl;
    string_view resultView;

    std::exception_ptr eptr;
    try {
        Status s = _db->Get(ReadOptions(), operand->key(), &result);
        printStatus(s, "get");

        resultView = result;
        cout << "[RocksDBStore::get] key: " << operand->key() << ", value: " << result << ", resultView: " << resultView << endl;
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, this->toString());

    return result;
}

// (a, b]
void RocksDBStore::iterate(const Range *range, ServerWriter<Operand> *writer) {
    LOG(INFO) << "[RocksDBStore::iterate] dbDir: " << _dbDir << endl;

    int count = 0;

    std::exception_ptr eptr;
    try {
        rocksdb::Iterator *it  = _db->NewIterator(ReadOptions());

        string start = range->start();
        string end = range->end();
        long threshold = range->minchunksize() > 0 ? range->minchunksize() : PAYLOAD_THREASHOLD;
        string_view keyView, valueView;
        bool locateStart = !start.empty();
        bool checkEnd = !end.empty();
        Operand operand;
        long bytesCount = 0;
        long count = 0;

        if (locateStart) {
            it->SeekForPrev(start);
            it->Next();
        } else {
            it->SeekToFirst();
        }
        while (it->Valid()) {
            keyView = it->key().ToString();

            if (bytesCount >= threshold || (checkEnd && keyView.compare(end) > 0)) {
                break;
            }
            valueView = it->value().ToString();

            operand.set_key(keyView.data(), keyView.size());
            operand.set_value(valueView.data(), valueView.size());

            writer->Write(operand);

            bytesCount += keyView.size() + valueView.size();
            ++count;
            it->Next();
        }

        delete it;
        cout << "[RocksDBStore::iterate] dbDir: " << _dbDir << ", total iterated: " << count << endl;
        LOG(INFO) << "[RocksDBStore::iterate] dbDir: " << _dbDir << ", total iterated: " << count << endl;
    } catch (...) {
        eptr = std::current_exception();
    }

    handle_eptr(eptr, __FILE__, __LINE__, this->toString());
}

void RocksDBStore::iterateAll() {

    cout << "------ iterateAll ------" << endl;

    std::exception_ptr eptr;
    try {
        long i = 0;
        Iterator *it = _db->NewIterator(ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            cout << ++i << ": key: " << it->key().ToString() << ", value: " << it->value().ToString() << endl;
        }

        delete it;
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, this->toString());
}

string RocksDBStore::toString() {
    std::stringstream ss;
    std::exception_ptr eptr;
    try {
        ss << "{storeInfo: " << this->storeInfo.toString()
           << ", dbDir: " << this->_dbDir
           << "}";
    } catch (...) {
        eptr = std::current_exception();
    }
    handle_eptr(eptr, __FILE__, __LINE__, "error in RocksDBStore::toString()");

    string result;
    ss >> result;

    return result;
}