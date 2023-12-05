import traceback
from datetime import datetime


def exception_catch(func):
    def wrapper(*args, **kw):
        try:
            return func(*args, **kw)
        except Exception as e:
            msg = (
                f"\n\n==== detail start, at {_time_now()} ====\n"
                f"{traceback.format_exc()}"
                f"\n==== detail end ====\n\n"
            )
            print(msg)
            raise RuntimeError(msg) from e

    return wrapper


def _time_now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S %f")
