from eggroll.core.command.commands import CommandURI, _to_service_name


class RendezvousStoreCommands:
    prefix = 'v1/cluster-manager/job/rendezvous'

    set = 'set'
    set_service_name = _to_service_name(prefix, set)
    SET = CommandURI(set_service_name)

    get = 'get'
    get_service_name = _to_service_name(prefix, get)
    GET = CommandURI(get_service_name)

    add = 'add'
    add_service_name = _to_service_name(prefix, add)
    ADD = CommandURI(add_service_name)
