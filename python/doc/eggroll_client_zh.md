# Eggroll Client操作使用


关于Eggroll Client 的安装和使用。所有命令都有一个共同的调用入口，可以在命令行中输入 "eggroll" 来获取所有命令类别及其子命令.
如下界面：


    [IN]
    eggroll
    
    [OUT]
    Usage: eggroll [OPTIONS] COMMAND [ARGS]...
    
      Eggroll Client
    
    Options:
      --eggroll-properties PATH  eggroll properties file
      -h, --help                 Show this message and exit.
    
    Commands:
      init  Eggroll CLI 初始化
      task  Task命令操作


## 安装

    使用 pip 等工具直接安装相应版本，例如：
    
    pip install eggroll

## 初始化

    eggroll init -h 
    
    -详情: Eggroll CLI 初始化命令。配置有效的Eggroll服务的地址和端口.
       
    -用法: eggroll init [OPTIONS]

    Options:
      --ip TEXT       地址
      --port INTEGER  端口
      -h, --help      帮助.


## 命令

### 任务提交
submit a deepspeed task
```bash
eggroll task submit [OPTIONS]
```
**Options**

| 参数 | 简写 | 全写 | 是否必填 | 类型 | 详情 |
| :-------- |:-----|:-------------| :--- | :----- |------|
| num-gpus | - | `--num-gpus` | 是 | 数字 | 使用gpu数 |
| script-path | - | `--script-path` | 是 | 字符 | 使用脚本地址 |
| log-type | - | `--log-type` | 否 | 字符 | 选择输出日志类型 |
| conf | - | `--conf` | 否 | 字符 | 配置 |
| timeout-seconds | - | `--timeout-seconds` | 否 | 数字 | 超时时间 |

**示例**
```bash
eggroll task submit --num-gpus nums --script-path xxx --conf checkpoint=/xxx/xxx --conf data_path=/xxx/xxx --conf model_checkpoint_save_path=/xxx/xxx
```

### 任务查询

```bash
eggroll task  query [OPTIONS]
```
**选项**

| 参数 | 简写 | 全写 | 是否必填 | 类型 | 详情 |
| :-------- |:-----|:-------------| :--- | :----- |------|
| session-id | - | `--session-id` | 是 | 字符 | 任务ID |

**示例**
```bash
eggroll task  query --session-id xxx
```

### 任务杀掉
```bash
eggroll task kill [OPTIONS]
```
**选项**

| 参数 | 简写 | 全写 | 是否必填 | 类型 | 详情 |
| :-------- |:-----|:-------------| :--- | :----- |------|
| session-id | - | `--session-id` | 是 | 字符 | 任务ID |

**示例**
```bash
eggroll task  kill --session-id xxx
```

### 任务停止
```bash
eggroll task stop [OPTIONS]
```
**选项**

| 参数 | 简写 | 全写 | 是否必填 | 类型 | 详情 |
| :-------- |:-----|:-------------| :--- | :----- |------|
| session-id | - | `--session-id` | 是 | 字符 | 任务ID |

**示例**
```bash
eggroll task  stop --session-id xxx
```


### 下载数据
```bash
eggroll task download [OPTIONS]
```
**选项**

| 参数 | 简写 | 全写 | 是否必填 | 类型 | 详情 |
| :-------- |:-----|:-------------| :--- | :----- |------|
| session-id | - | `--session-id` | 是 | 字符 | 任务ID |
| content-type | - | `--content-type` | 否 | 数字 |  内容类型,默认:0, 可选择参数: 全部:0, 模型: 1, 日志: 2, 结果: 3|
| download-dir | - | `--download-dir` | 是 | 字符 | 保存路径 |
| ranks | - | `--ranks` | 是 | 字符 | 等级,可选择参数:[0,1,2..] |

**示例**
```bash
eggroll task  download --session-id xxx --content-type xxx --download-dir xxx --ranks 0
```


### 日志获取
```bash
eggroll task get-log [OPTIONS]
```
**Options**

| 参数 | 简写 | 全写 | 是否必填 | 类型 | 详情 |
| :-------- |:-----|:-------------| :--- | :----- |------|
| session-id | - | `--session-id` | 是 | 字符 | 任务ID |
| rank | - | `--ranks` | 否 | 字符 | 等级,默认:0,可可选择:0,1,2.. |
| path | - | `--path` | 否 | 字符 |  内容类型,默认:0;可选择: ALL:0, MODELS: 1, LOGS: 2, RESULT: 3|
| tail | - | `--tail` | 否 | 数字 | 获取日志起始行,默认:100 |
| log-type | - | `--log-type` | 否 | 字符 | 日志类型,默认:stdout,可选择: stdout, stderr |

**示例**
```bash
eggroll task get-log --session-id xxx --rank 0 --path xxx/xxx --tail 100 --log-type stdout
```







