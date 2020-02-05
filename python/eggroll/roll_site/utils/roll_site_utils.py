#  Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#

from eggroll.core.transfer_model import ErFederationHeader


OBJECT_STORAGE_NAME = "__federation__"
DELIM = "#"

# TODO:1: consider moving it to ErFederationHeader class
def create_store_name(federation_header: ErFederationHeader):
    return DELIM.join([OBJECT_STORAGE_NAME,     # todo:0: remove this field
                       federation_header._federation_session_id,
                       federation_header._name,
                       federation_header._tag,
                       federation_header._src_role,
                       federation_header._src_party_id,
                       federation_header._dst_role,
                       federation_header._dst_party_id])
