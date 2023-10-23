#!/bin/bash

# supervisor 配置
confName=eggroll
moduleName=eggroll
version=3.0.0
# 上传的临时目录
uploadPath=/data/temp/eggroll30
# 项目部署路径
deployPath=/data/projects/fate/eggroll30
# 需要复制的目录
module_dirs=(config bin lib data)

# AOMP发布模板
# http://uat.aomp.weoa.com/static/index#/aomp/publishList/publishDetail/49834

aido() {

  action=$1

  case $action in

    stop)
      source /data/projects/fate/bin/init_env.sh
      /bin/bash /data/projects/fate/eggroll30/bin/eggroll.sh all stop

      echo "=======$action over=========="
    ;;
    init)
      cp -r /data/temp/eggroll30 /data/projects/fate/eggroll30

      echo "=======$action over=========="
    ;;

    replace)
      cp /data/temp/eggroll30/bin/*.jar /data/projects/fate/eggroll30/bin/
      echo "=======$action over=========="
    ;;

    start)
      cp /data/projects/fate/eggroll30/bin/eggroll.sh all start
      echo "=======$action over=========="
    ;;

    *)
     echo "Usage: start|stop"
    ;;

  esac

}





action=$1
case $action in

    start)
      aido stop
      aido backup
      aido cp
      aido replace
      aido start
      echo "#------over--------"
    ;;

    stop)
      aido stop
    ;;

    *)
      echo "Usage: start|stop"
    ;;

esac
