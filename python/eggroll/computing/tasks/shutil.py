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

# shutil is a standard library module to copy, move, and remove files and directories.
# this operation is dangerous if not used carefully.
# we will not use shutil in eggroll, but provide a wrapper to make it safer to use and easier to track.

import logging
import shutil

L = logging.getLogger(__name__)


def rmtree(path, ignore_errors=False, onerror=None):
    return shutil.rmtree(path, ignore_errors=ignore_errors, onerror=onerror)
