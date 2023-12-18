from datetime import datetime


_JOB_ID_DEFAULT_DATETIME_FORMAT = "%Y%m%d%H%M%S%f"
_DELIM = "/"


def _time_now_ns():
    return datetime.now().strftime(_JOB_ID_DEFAULT_DATETIME_FORMAT)


def generate_job_id(session_id: str, tag: str, description: str = None):
    job_id = _DELIM.join([session_id, "job", tag, _time_now_ns()])
    if description:
        job_id = _DELIM.join([job_id, description])
    return job_id


def generate_task_id(job_id, partition_id):
    return _DELIM.join([job_id, "task", str(partition_id)])
