#!/bin/bash

# -d 指定配置文件
#  -d,--defaults <environment file>      The environment properties with which
#                                            every new session is initialized.
#                                            Properties might be overwritten by
#                                            session properties.

# -l 指定 flinkjob 的 classpath, 有了 -l 我们就可以把各种 connector 的 jar 文件放到这里
# 而不用放到 ${FLINK_HOME}/lib 下相互影响
# -l,--library <JAR directory>          A JAR file directory with which every
#                                            new session is initialized. The files
#                                            might contain user-defined classes
#                                            needed for the execution of
#                                            statements such as functions, table
#                                            sources, or sinks. Can be used
#                                            multiple times.

${FLINK_HOME}/bin/sql-client.sh embedded -d ${FLINK_HOME}/conf/sql-client-conf.yaml -l ${SQL_CLIENT_HOME}/lib

# 完事，记得将该文件标记为 可执行的 x 文件