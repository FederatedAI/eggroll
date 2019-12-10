#
#  Copyright 2019 The Eggroll Authors. All Rights Reserved.
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

from eggroll.roll_site import RuntimeInstance
from eggroll.roll_site import cluster as cluster_rollsite

class RollSite:
  def __init__(self, ctx=None, opts={}):
    self.runtime_conf_path =
    self.server_conf_path =
    self.transfer_conf_path =
    self.rollsite = cluster_rollsite.init(job_id=job_id, runtime_conf_path=runtime_conf,
                                          server_conf_path=server_conf_path, transfer_conf_path=transfer_conf_path)

  def push(self,obj, name: str, tag: str, role=None, idx=-1):
      return rollsite.remote(obj=obj, name=name, tag=tag)

  def pull(self,obj, name: str, tag: str, role=None, idx=-1):
      return rollsite.get(obj, name=name)


