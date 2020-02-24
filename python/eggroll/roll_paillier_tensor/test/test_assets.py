import unittest

from eggroll.core.meta_model import ErEndpoint
from eggroll.roll_site.roll_site import RollSiteContext
import os
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


def get_debug_rs_context(rp_context, rs_session_id, options):
    return RollSiteContext(rs_session_id, rp_ctx=rp_context, options=options)


def generate_dataset(src_path, target_path, id_col, target_count=10000):
    src_list = []
    header = None
    with open(src_path) as src_fd:
        idx = 0
        for line in src_fd:
            if idx == 0:
                header = line
            else:
                src_list.append(line.split(","))
            idx += 1
    with open(target_path, "w") as target_fd:
        target_fd.write(header)
        idx = 1
        for _ in range(0, target_count // len(src_list) + 1):
            for row in src_list:
                if idx > target_count:
                    break
                row[id_col] = str(idx)
                target_fd.write(",".join(row))
                idx += 1

test_data_dir = os.environ["EGGROLL_HOME"] + "/" + "data/test_data/"


class TestAssets(unittest.TestCase):

    def test_gen_breast_a_10000(self):
        generate_dataset(test_data_dir + "breast_a.csv", test_data_dir + "breast_a_10000.csv", 0, 10000)

    def test_gen_breast_b_10000(self):
        generate_dataset(test_data_dir + "breast_b.csv", test_data_dir + "breast_b_10000.csv", 0, 10000)