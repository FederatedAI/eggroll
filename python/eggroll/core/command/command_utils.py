import logging
from time import monotonic, sleep

L = logging.getLogger(__name__)


def command_call_retry(callale, args, retry_max=3, retry_interval=1, retry_timeout=10):
    retry_count = 0
    endtime = monotonic() + retry_timeout
    while True:
        try:
            return callale(*args)
        except Exception as e:
            L.warning(f"session init failed: {e}, retrying...")
            retry_count += 1
            if retry_count < retry_max and monotonic() < endtime:
                sleep(retry_interval)
            else:
                raise e
