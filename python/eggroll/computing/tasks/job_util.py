from eggroll.core.utils import time_now_ns


def generate_job_id(session_id, tag="", delim="-"):
    result = delim.join([session_id, "py", "job", time_now_ns()])
    if not tag:
        return result
    else:
        return f"{result}_{tag}"


def generate_task_id(job_id, partition_id, delim="-"):
    return delim.join([job_id, "task", str(partition_id)])
