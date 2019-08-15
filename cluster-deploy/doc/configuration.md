[TOC]

# 1. Overall
This document consists of 7 modules:

1. Clustercomm;
2. Meta-service;
3. Proxy;
4. Roll;
5. Storage-service-cxx;
6. Api;
7. Egg.

# 2. Module Configurations

## 2.1. Clustercomm
Clustercomm module handles task data communication (i.e. 'clustercomm') among different parties for own party.
### 2.1.1. applicationContext-clustercomm.xml
No modification is required.
### 2.1.2. log4j2.properties
No modification is required.
### 2.1.3. clustercomm.properties
Item              | Meaning             | Example / Value
------------------|---------------------|------------------------
party.id          |party id of FL participant      | e.g. 10000
service.port      |port to listen on    | clustercomm defaults to 9394 
meta.service.ip   |meta-service ip  | e.g. 127.0.0.xx 
meta.service.port |meta-service port | defaults to 8590
eggroll.compatible.enabled |compatibility enabled | true 

## 2.2. Meta-Service
Meta-Service module stores metadata required by eggroll.
### 2.2.1. applicationContext-meta-service.xml
No modification is required.
### 2.2.2. log4j2.properties
No modification is required.
### 2.2.3. meta-service.properties
Item              | Meaning             | Example / Value
------------------|---------------------|------------------------
party.id          |party id of FL participant | e.g. 10000
service.port      |port to listen on    | meta-service defaults to 8590
jdbc.driver.classname |jdbc driver's classname | recommendation: com.mysql.cj.jdbc.Driver 
jdbc.url |jdbc connection url | modify as needed 
jdbc.username |database username | modify as needed 
jdbc.password |database password | modify as needed 
target.project |target project. Required by mybatis-generator | fixed to meta-service 
eggroll.compatible.enabled |compatibility enabled | true 
### 2.2.4. Database Configurations
#### 2.2.4.1. Database and Tables Creation
Please run the following SQL in this project:
framework/meta-service/src/main/resources/create-meta-service.sql

#### 2.2.4.2. Adding Node Infomation
To deploy Eggroll in a distributed environment (i.e. cluster deploy), following modules are minimum for **1** party:

Module | Minimum requirement | Comments
-------|---------------------|------------
Roll   | exactly 1           | Advanced deployment in the next version
Egg (processor) | at least 1 | This will change in the next version
Egg (storage-service) | at least 1 | 
Clustercomm | exactly 1       | 
Proxy  | at least 1          | 
Exchange | Inter-party communication can use any amount of exchange, included 0 (i.e. direct connection)|

**a. Roll**

For each Roll, a database record should be inserted:
```
INSERT INTO node (ip, port, type, status) values 
('${roll_ip}', '${roll_port}', 'ROLL', 'HEALTHY')
```

**b. Processor**

For each Processor, a database record should be inserted:
```
INSERT INTO node (ip, port, type, status) values 
('${processor_ip}', '${processor_port}', 'EGG', 'HEALTHY')
```

**c. Storage-Service**

For each Storage-Service, a database record should be inserted:
```
INSERT INTO node (ip, port, type, status) values 
('${storage_service_ip}', '${storage_service_port}', 'STORAGE', 'HEALTHY')
```

**d. Clustercomm**  

No database record insertion is need for Clustercomm module at this stage.

**e. Proxy**

For each Proxy, a database record should be inserted:
```
INSERT INTO node (ip, port, type, status) values 
('${proxy_ip}', '${proxy_port}', 'PROXY', 'HEALTHY')
```




## 2.3. Proxy (Shared with Exchange For Now)
Proxy (Exchange) is communication channel among parties.
### 2.3.1. applicationContext-proxy.xml
No modification is required.
### 2.3.2. log4j2.properties
No modification is required.
### 2.3.3. proxy.properties
Item              | Meaning                | Example / Value
------------------|------------------------|------------------------
coordinator       | same as party id       | e.g. 10000
ip                | ip to bind (in multi-interface env) | optional
port              | port to listen on      | proxy (exchange) defaults to 9370
route.table       | path to route table    | modify as needed
server.crt        | server certification path | only necessary in secure communication
server.key        | server private key path | only necessary in secure communication
root.crt          | path to certification of root ca | default null 
eggroll.compatible.enabled | compatibility enabled | true 

### 2.3.4. route_table.json

Item              | Meaning                       | Example / Value
------------------|------------------------------|------------------------
default           | ip and port of exchange or default proxy | 127.0.0.xx / 9370 
${partyId}        | clustercomm ip and port of own party | 127.0.0.yy / 9394 

example:

```
{
  "route_table": {
    "default": {
      "default": [
        {
          "ip": "127.0.0.1",
          "port": 9370
        }
      ]
    },
    "10000": {
      "eggroll": [
        {
          "ip": "127.0.0.1",
          "port": 9370
        }
      ]
    }
   },
  "permission": {
    "default_allow": true
  }
}
```


## 2.4. Roll
Roll module is responsible for accepting distributed job submission, job / data schedule and result aggregations.

### 2.4.1. applicationContext-roll.xml
No modification is required.
### 2.4.2. log4j2.properties
No modification is required.
### 2.4.3. roll.properties
Item              | Meaning             | Example / Value
------------------|---------------------|------------------------
party.id          |party id of FL participant | e.g. 10000
service.port      |port to listen on.   | roll defaults to 8011
meta.service.ip   |meta-service ip      | e.g. 127.0.0.xx 
meta.service.port |meta-service port    | defaults to 8590
eggroll.compatible.enabled |compatibility enabled | true 

## 2.5. Storage-Service-cxx
Storage-Service-cxx module handles data storage on that single node.
### 2.5.1. services.sh
No modification is required. But there are 2 command line mandatory arguments:

Item                   | Meaning             | Example / Value
-----------------------|---------------------|------------------------
PORT                   | port to listen on   | storage-service-cxx defaults to 7778 
DATADIR                |data dir           | must be the same with processor's data dir


## 2.6. API
APIs are interfaces exposed by the whole running architecture. Algorithm engineers / scientists can utilize Eggroll framework via API.
### 2.6.1 api/eggroll/conf/server_conf.json
```
{
  "servers": {
    "roll": {
      "host": "localhost",  # ip address of roll module
      "port": 8011          # port of roll module
    },
    "clustercomm": {
      "host": "localhost",  # ip address of clustercomm module
      "port": 9394          # port of clustercomm module
    },
    "proxy": {
       "host": "localhost", # ip address of proxy module
       "port": 9370         # port address of proxy module
    }
  }
}
```

## 2.7. Egg

Egg used to execute user-defined functions.

### 2.7.1. applicationContext-egg.xml

No modification is required.

### 2.7.2. log4j2.properties

No modification is required.

### 2.7.3 egg/conf/egg.properties

| Item                                          | Meaning                          | Example / Value                                 |
| --------------------------------------------- | -------------------------------- | ----------------------------------------------- |
| party.id                                      | party id of FL participant       | e.g. 10000                                      |
| service.port                                  | port to listen on.               | egg defaults to 7888                            |
| eggroll.computing.engine.names                | computing engine name of eggroll | defaults to processor                           |
| eggroll.computing.processor.bootstrap.script  | path of processor-starter.sh     | modify as needed                                |
| eggroll.computing.processor.start.port        | port of processor start          | defaults to 50000                               |
| eggroll.computing.processor.venv              | path of python virtualenv        | modify as needed                                |
| eggroll.computing.processor.engine-path       | path of processor.py             | modify as needed                                |
| eggroll.computing.processor.data-dir          | data dir                         | must be the same with storage's data dir        |
| eggroll.computing.processor.logs-dir          | log dir of processor             | modify as needed                                |
| eggroll.computing.processor.session.max.count | the count of processors          | modify as needed, but no more than server cores |

