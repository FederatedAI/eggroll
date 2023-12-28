import traceback
from datetime import datetime


def get_stack():
    return (
        f"\n\n==== stack start, at {time_now('%Y-%m-%d %H:%M:%S %f')}"
        f"{''.join(traceback.format_stack())}"
        f"\n==== stack end ====\n\n"
    )


DEFAULT_DATETIME_FORMAT = "%Y%m%d%H%M%S%f"


def time_now(format: str = DEFAULT_DATETIME_FORMAT):
    formatted = datetime.now().strftime(format)
    if format == DEFAULT_DATETIME_FORMAT or ("%f" in format):
        return formatted[:-3]
    else:
        return formatted


def get_self_ip():
    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(("10.255.255.255", 1))
        self_ip = s.getsockname()[0]
    except:
        self_ip = "127.0.0.1"
    finally:
        s.close()
    return self_ip
