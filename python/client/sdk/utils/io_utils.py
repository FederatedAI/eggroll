#
#  Copyright 2019 The FATE Authors. All Rights Reserved.
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
import os
import tarfile
import uuid
from tempfile import TemporaryDirectory


def download_from_request(http_response, download_path):
    try:
        with TemporaryDirectory() as output_tmp_dir:
            path = os.path.join(output_tmp_dir, str(uuid.uuid1()))
            with open(path, 'wb') as fw:
                for chunk in http_response.iter_content(1024):
                    if chunk:
                        fw.write(chunk)
            tar = tarfile.open(path, "r:gz")
            file_names = tar.getnames()
            for file_name in file_names:
                tar.extract(file_name, download_path)
            tar.close()
            return {"code": 0, "message": f"download success, please check the path: {download_path}"}
    except Exception as e:
        return {"code": 100, "message": f"download failed, {str(e)}"}

