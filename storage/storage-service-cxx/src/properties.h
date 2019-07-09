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

#ifndef STORAGE_SERVICE_CXX_PROPERTIES_H
#define STORAGE_SERVICE_CXX_PROPERTIES_H

#include <string>
#include <map>
#include <iterator>

class Properties {
public:
    bool set(std::string key, std::string value) {
        store[key] = value;
        return true;
    }

    std::string get(std::string key) {
        std::string result;
        auto iter = store.find(key);

        if (iter != store.end()) {
            result = iter->second;
        } else {
            result = "";
        }

        return result;
    }
private:
    std::map<const std::string, std::string> store;
};

#endif //STORAGE_SERVICE_CXX_PROPERTIES_H
