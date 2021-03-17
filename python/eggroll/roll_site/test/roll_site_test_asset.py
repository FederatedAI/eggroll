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


import configparser
import os

import eggroll.roll_pair.test.roll_pair_test_assets as rpta
from eggroll.core.conf_keys import RollSiteConfKeys, SessionConfKeys, \
    CoreConfKeys
from eggroll.core.constants import StoreTypes, DeployModes
from eggroll.core.meta_model import ErStore, ErStoreLocator, ErEndpoint
from eggroll.roll_site.roll_site import RollSiteContext

is_standalone = False
manager_port_guest = 4671
egg_port_guest = 20003
transfer_port_guest = 20004
manager_port_host = 4670
egg_port_host = 20001
transfer_port_host = 20002

EGGROLL_HOME = os.environ['EGGROLL_HOME']

default_props_file = f"{EGGROLL_HOME}/conf/eggroll.properties"


def get_option(role, self_party_id=None, conf_file=default_props_file, deploy_mode=DeployModes.CLUSTER):
    print(f'conf file: {conf_file}')
    configs = configparser.ConfigParser()

    configs.read(conf_file)
    eggroll_configs = configs['eggroll']

    options = {}

    if self_party_id is None:
        party_id = eggroll_configs[RollSiteConfKeys.EGGROLL_ROLLSITE_PARTY_ID.key]
    else:
        party_id = self_party_id

    options['self_party_id'] = party_id
    options['self_role'] = role

    options['proxy_endpoint'] = \
        ErEndpoint(host=eggroll_configs[RollSiteConfKeys.EGGROLL_ROLLSITE_HOST.key],
                   port=int(eggroll_configs[RollSiteConfKeys.EGGROLL_ROLLSITE_PORT.key]))

    options[RollSiteConfKeys.EGGROLL_ROLLSITE_DEPLOY_MODE.key] = deploy_mode

    return options


host_ip = 'localhost'
guest_ip = 'localhost'
host_options = {'self_role': 'host',
                'self_party_id': 10001,
                'proxy_endpoint': ErEndpoint(host=host_ip, port=9395),
                }

guest_options = {'self_role': 'guest',
                 'self_party_id': 10002,
                 'proxy_endpoint': ErEndpoint(host=guest_ip, port=9396),
                 }



ER_STORE1 = ErStore(
        store_locator=ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LEVELDB,
                                     namespace="namespace",
                                     name="name"))


#roll_site_session_id = f'atest'


def get_debug_test_context(is_standalone=False,
        manager_port=4670,
        command_port=20001,
        transfer_port=20002,
        role='host',
        self_party_id=None,
        props_file=default_props_file,
        roll_site_session_id=None):
    rp_context = rpta.get_debug_test_context(is_standalone=is_standalone,
                                             manager_port=manager_port,
                                             egg_port=command_port,
                                             transfer_port=transfer_port,
                                             session_id='_'.join([roll_site_session_id, role, str(self_party_id)]))

    rs_context = RollSiteContext(roll_site_session_id=roll_site_session_id,
                                 rp_ctx=rp_context,
                                 options=get_option(role=role, self_party_id=self_party_id, conf_file=props_file))

    return rs_context


def get_standalone_context(role, self_party_id, props_file=default_props_file, roll_site_session_id=None):
    options = {}
    options[SessionConfKeys.CONFKEY_SESSION_DEPLOY_MODE] = DeployModes.STANDALONE
    options[CoreConfKeys.STATIC_CONF_PATH] = props_file
    os.environ["EGGROLL_RESOURCE_MANAGER_AUTO_BOOTSTRAP"] = "1"

    rp_context = rpta.get_standalone_context(options=options)

    rs_options = get_option(role, self_party_id, props_file, deploy_mode=DeployModes.STANDALONE)
    options.update(rs_options)
    rs_context = RollSiteContext(roll_site_session_id, rp_ctx=rp_context,
                                 options=get_option(role, self_party_id, props_file, deploy_mode=DeployModes.STANDALONE))

    return rs_context


def get_cluster_context(role,
        options: dict = None,
        props_file=default_props_file,
        party_id=None,
        roll_site_session_id=None):
    if options is None:
        options = {}
    options['session_id'] = '_'.join([roll_site_session_id, role, party_id])
    print("session_id: ", options['session_id'])

    options[CoreConfKeys.STATIC_CONF_PATH] = props_file
    rp_context = rpta.get_cluster_context(options=options)

    rs_options = get_option(role, self_party_id=party_id, conf_file=props_file)

    rs_context = RollSiteContext(roll_site_session_id, rp_ctx=rp_context,
                                 options=rs_options)

    return rs_context


if __name__ == "__main__":
    options = get_option()
    print(options)
