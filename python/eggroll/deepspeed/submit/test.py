from eggroll.core.proto.command_pb2 import CommandRequest

if __name__ == "__main__":
    # deepspeedJob = DeepspeedJob(session_id="deepspeed_session_20230705-175508-766715",host="localhost",port=4670)
    # deepspeedJob.download_job_to()
    # print(DsDownloadResponse(session_id="xxxxxxx").SerializeToString())
    # DsDownloadResponse.MergeFromString(b'\n\x07xxxxxxx')

    print(CommandRequest(uri="xxxx").SerializeToString())
    data = CommandRequest(uri="xxxx").SerializeToString()
    commandRequest = CommandRequest()
    (commandRequest.ParseFromString(data))
    print(commandRequest)
