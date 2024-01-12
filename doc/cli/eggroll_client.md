# Eggroll Client

Description

Introduces how to install and use the eggroll Client. 

The command line provided by Eggroll Client, all commands will
have a common invocation entry, you can type eggroll in the command line to get all
the command categories and their subcommands.

    [IN]
    eggroll
    
    [OUT]
    Usage: eggroll [OPTIONS] COMMAND [ARGS]...
    
      Eggroll Client
    
    Options:
      --eggroll-properties PATH  eggroll properties file
      -h, --help                 Show this message and exit.
    
    Commands:
      init  Eggroll CLI Init Command
      task  Task Operations


## Install Eggroll Client
Eggroll Client will be distributed to pypi, you can install the corresponding version
directly using tools such as pip, e.g.

    pip install eggroll

## Init Eggroll Client

    eggroll init -h 
    
    -description: Eggroll CLI Init Command. provide ip and port of a valid eggroll server.
       
    -Usage: eggroll init [OPTIONS]

    Options:
      --ip TEXT       Eggroll gip address.
      --port INTEGER  Eggroll grpc port.
      -h, --help      Show this message and exit.


### Eggroll Client

After successfully installed Eggroll Client,and init Client. user needs to configure
server information for Client. Client cli provides a command line tool for quick setup. 
Run the following command for more information.


## Interface of Eggroll Client Cli

### submit
submit a deepspeed task
```bash
eggroll task submit [OPTIONS]
```
**Options**

| parameters | short-format | long-format | required | type | description |
| :-------- |:-----|:-------------| :--- | :----- |------|
| num-gpus | - | `--num-gpus` | yes | int | Use Gpu Num |
| script-path | - | `--script-path` | yes | str | Script Path |
| log-type | - | `--log-type` | no | str | Output Log Type |
| conf | - | `--conf` | no | str | conf |
| timeout-seconds | - | `--conf` | no | int | Queue Timeout(s) |

**Usage**
```bash
eggroll task submit --num-gpus nums --script-path xxx --conf checkpoint=/xxx/xxx --conf data_path=/xxx/xxx --conf model_checkpoint_save_path=/xxx/xxx
```

### query
query task
```bash
eggroll task  query [OPTIONS]
```
**Options**

| parameters | short-format | long-format | required | type | description |
| :-------- |:-----|:-------------| :--- | :----- |------|
| session-id | - | `--session-id` | yes | str | Session ID for the task |

**Usage**
```bash
eggroll task  query --session-id xxx
```

### kill task
```bash
eggroll task kill [OPTIONS]
```
**Options**

| parameters | short-format | long-format | required | type | description |
| :-------- |:-----|:-------------| :--- | :----- |------|
| session-id | - | `--session-id` | yes | str | Session ID for the task |

**Usage**
```bash
eggroll task  kill --session-id xxx
```

### stop task
```bash
eggroll task stop [OPTIONS]
```
**Options**

| parameters | short-format | long-format | required | type | description |
| :-------- |:-----|:-------------| :--- | :----- |------|
| session-id | - | `--session-id` | yes | str | Session ID for the task |

**Usage**
```bash
eggroll task  stop --session-id xxx
```


### download data
```bash
eggroll task download [OPTIONS]
```
**Options**

| parameters | short-format | long-format | required | type | description |
| :-------- |:-----|:-------------| :--- | :----- |------|
| session-id | - | `--session-id` | yes | str | Session ID for the task |
| content-type | - | `--content-type` | no | int |  Content type,default:0, option: ALL:0, MODELS: 1, LOGS: 2, RESULT: 3|
| download-dir | - | `--download-dir` | yes | str | download path |
| ranks | - | `--ranks` | yes | str | Ranks,options:[0,1,2..] |

**Usage**
```bash
eggroll task  download --session-id xxx --content-type xxx --download-dir xxx --ranks 0
```


### get log
```bash
eggroll task get-log [OPTIONS]
```
**Options**

| parameters | short-format | long-format | required | type | description |
| :-------- |:-----|:-------------| :--- | :----- |------|
| session-id | - | `--session-id` | yes | str | Session ID for the task |
| rank | - | `--ranks` | no | str | Log Rank,default:0,Options:0,1,2.. |
| path | - | `--path` | no | str |  Content type,default:0;option: ALL:0, MODELS: 1, LOGS: 2, RESULT: 3|
| tail | - | `--tail` | no | int | log tail line,default:100 |
| log-type | - | `--log-type` | no | str | log Type,default:stdout,options: stdout, stderr |

**Usage**
```bash
eggroll task get-log --session-id xxx --rank 0 --path xxx/xxx --tail 100 --log-type stdout
```







