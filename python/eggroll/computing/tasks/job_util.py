from datetime import datetime


_JOB_ID_DEFAULT_DATETIME_FORMAT = "%Y%m%d.%H%M%S.%f"


def _time_now_ns():
    return datetime.now().strftime(_JOB_ID_DEFAULT_DATETIME_FORMAT)


def generate_job_id(session_id, tag="", delim="-"):
    result = delim.join([session_id, "py", "job", _time_now_ns()])
    if not tag:
        return result
    else:
        return f"{result}_{tag}"


def generate_task_id(job_id, partition_id, delim="-"):
    return delim.join([job_id, "task", str(partition_id)])
