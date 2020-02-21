from eggroll.core.meta_model import ErEndpoint
from eggroll.roll_site.roll_site import RollSiteContext

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
