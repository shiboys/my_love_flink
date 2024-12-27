FROM flink:1.11.1-scala_2.11-java11
# flink:1.11.1 docker daemon 启动，有 bug ，无法找到 jvm 

#RUN mkdir -p /opt/flink/job/lib
COPY target/flink*.jar lib/job.jar
#RUN mkdir -p /opt/flink/sql-client/lib
# RUN mkdir -p /opt/flink/backend

#Download connnector libraries

# RUN wget -P /opt/flink/sql-client/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka_2.11/1.11.1/flink-connector-kafka_2.11-1.11.1.jar; \
#     wget -P /opt/flink/sql-client/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-elasticsearch7_2.11/1.11.1/flink-connector-elasticsearch7_2.11-1.11.1.jar; \
#     wget -P /opt/flink/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc_2.11/1.11.1/flink-connector-jdbc_2.11-1.11.1.jar; \
#     wget -P /opt/flink/sql-client/lib/ https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar;

# copy configuration
COPY  conf/* /opt/flink/conf/

# WORKDIR /opt/flink/sql-client
# ENV SQL_CLIENT_HOME /opt/flink/sql-client

COPY docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]