FROM maven:3.6-jdk-8-slim as builder

###############################################################################
# Build SQL Playground Image
###############################################################################

FROM flink:1.11.1-scala_2.11

# Copy sql-client script

COPY bin/* /opt/sql-client/

RUN mkdir -p /opt/sql-client/lib

# Downlaod connector libraries

# wget 这个命令 docker 环境默认不会安装，在安装了 flink 的官方镜像之后，改命令就可以使用了，我看官方镜像用了这个指令，我推测是这样的
# flink 1.11.1 的官方镜像地址 https://hub.docker.com/layers/library/flink/1.11.1-scala_2.11/images/sha256-5f2903da2e9d1fcbbdf4a6308e32b829de0e20415f04f3ca7d0ec66273d0a661?context=explore
# 官方的 DockerFile 源码如下 https://github.com/apache/flink-docker/blob/dev-1.11/Dockerfile-debian.template
# 我们这里 下载了 mysql 8.0 的jar 包，但是 mysql 的 docker 镜像用的是 mysql 5.7 版本，这应该是兼容的，在 编写 Flink Sql 的 DDL 语句的时候尤其要注意 'connector.Driver' ，可以省略掉这个 with 选项
RUN wget -P /opt/sql-client/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7_2.11/1.11.1/flink-sql-connector-elasticsearch7_2.11-1.11.1.jar; \
    wget -P /opt/sql-client/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.11.1/flink-sql-connector-kafka_2.11-1.11.1.jar; \
    wget -P /opt/sql-client/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc_2.11/1.11.1/flink-connector-jdbc_2.11-1.11.1.jar; \
    wget -P /opt/sql-client/lib https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar;

# Copy Configurations

COPY conf/* /opt/flink/conf/

WORKDIR /opt/sql-client
ENV SQL_CLIENT_HOME /opt/sql-client

COPY docker-entrypoint.sh /
ENTRYPOINT [ "/docker-entrypoint.sh" ]