#!/bin/bash

# 这个文件必须好好写，否则 flink 的 stdout 就只能通过 docker logs container_id 进行查看

## If unspecified， the hostname of the container is taken as the Jobmanager address
FLINK_HOME=${FLINK_HOME:-"/opt/flink/bin"}

JOB_CLUSTER="job-cluster"
TASK_MANAGER="task-manager"

# 第一命令
CMD="$1"
# 命令解析后移一位
shift;

if [ "${CMD}" == "--help" -o "${CMD}" == "-h" ]; then
# basename 返回当前文件路径的最后一个路径，如果 $0 只是一个文件，则返回 $0
    echo "Usage: $(basename $0) (${JOB_CLUSTER}|${TASK_MANAGER})"
    exit 0
elif [ "${CMD}" == "${JOB_CLUSTER}" -o "${CMD}" == "${TASK_MANAGER}" ]; then
    echo "Starting ${CMD}"

    if [ "${CMD}" == "${TASK_MANAGER}" ]; then
        # 不使用官方的 taskmanager.sh start-foreground 而使用 后台启动的方式，外加 前台启动的 tail -f 
        # 命令将 docker 保持运行并且将 flink 的日志和 stdout 输出到 log 文件夹下
        exec ${FLINK_HOME}/bin/taskmanager.sh start-foreground "$@"
    else
        exec ${FLINK_HOME}/bin/standalone-job.sh start-foreground "$@"
    fi
fi

sleep 1

exec "$@"
# exec /bin/bash -c "tail -f $FLINK_HOME/log/*.log"

# tail -f /dev/null