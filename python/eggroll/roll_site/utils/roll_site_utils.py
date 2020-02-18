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

from eggroll.core.transfer_model import ErRollSiteHeader


OBJECT_STORAGE_NAME = "__federation__"
DELIM = "#"


# TODO:1: consider moving it to ErRollSiteHeader class
def create_store_name(roll_site_header: ErRollSiteHeader):
    return DELIM.join([OBJECT_STORAGE_NAME,  # todo:0: remove this field
                       roll_site_header._roll_site_session_id,
                       roll_site_header._name,
                       roll_site_header._tag,
                       roll_site_header._src_role,
                       roll_site_header._src_party_id,
                       roll_site_header._dst_role,
                       roll_site_header._dst_party_id])
