## Flink Sql 使用 Docker Compose 启动过程中疑难杂症记录

### 其他容器正常启动，只有一两个容器无法启动

解决办法主要分为一下几步：
1、 移除配置异常的 docker 镜像
```sh
# 先使用 docker compose rm image_name 的方式移除掉配置错误或者无法启动的镜像
docker compose -f docker-compose-sql-client.yml rm kafka
```
输出如下信息
```text
? Going to remove flink-sql-docker-demo-kafka-1 Yes
[+] Running 1/0
 ⠿ Container flink-sql-docker-demo-kafka-1  Removed                                                                                     0.0s
```

2、修改相关 docker-compose.yml 文件，将相关配置更改正确，然后以 --no-start 方式启动 相关镜像

```sh
# 先试用 ps 命令观察下上面的移除命令是否生效
docker compose -f docker-compose-sql-client.yml ps 

# 使用 --no-start 选项和 up 命令来重新创建之前启动失败的镜像的容器
docker compose -f docker-compose-sql-client.yml up --no-start

# 在这之后使用 restart 命令启动镜像
docker compose -f docker-compose-sql-client.yml restart kafka
```

输出如下信息：

```text
[+] Running 1/1
 ⠿ Container flink-sql-docker-demo-kafka-1  Started                                                                                     0.6s
```

3、查看最终的 docker 镜像进程, 观察之前启动失败的镜像是否已经成功启动

```sh
docker ps -a
```

### 在 docker 外部启动 kafka-console-producer 和 kafka-console-consumer

`dockek ps -a` 的输出如下：

```text
shiweijie@localhost flink-sql-docker-demo % docker ps -a
CONTAINER ID   IMAGE                                                 COMMAND                  CREATED       STATUS       PORTS                                            NAMES
4348159cadf4   wurstmeister/kafka:2.12-2.2.1                         "start-kafka.sh"         2 hours ago   Up 2 hours   0.0.0.0:9092->9092/tcp, 0.0.0.0:9094->9094/tcp   flink-sql-docker-demo-kafka-1
8282ee914c08   flink-sql-client-env:0.1                              "/docker-entrypoint.…"   3 hours ago   Up 3 hours   6123/tcp, 8081/tcp                               flink-sql-docker-demo-sql-client-1
551b625299d1   flink:1.11.1-scala_2.11                               "/docker-entrypoint.…"   3 hours ago   Up 3 hours   6123/tcp, 8081/tcp                               flink-sql-docker-demo-taskmanager-1
b818c67d83c8   mysql:5.7                                             "docker-entrypoint.s…"   3 hours ago   Up 3 hours   0.0.0.0:3306->3306/tcp, 33060/tcp                mysql
c544b6cd4392   docker.elastic.co/kibana/kibana:7.6.0                 "/usr/local/bin/dumb…"   3 hours ago   Up 3 hours   0.0.0.0:5601->5601/tcp                           flink-sql-docker-demo-kibana-1
8187311ebe3b   docker.elastic.co/elasticsearch/elasticsearch:7.6.0   "/usr/local/bin/dock…"   3 hours ago   Up 3 hours   0.0.0.0:9200->9200/tcp, 0.0.0.0:9300->9300/tcp   flink-sql-docker-demo-elasticsearch-1
f157c2a42f7f   flink:1.11.1-scala_2.11                               "/docker-entrypoint.…"   3 hours ago   Up 3 hours   6123/tcp, 0.0.0.0:8081->8081/tcp                 flink-sql-docker-demo-jobmanager-1
7f5bfcbbca62   zookeeper:3.4.9
```

从上面的输出可以看到, kafka 的镜像名称为 `flink-sql-docker-demo-kafka-1`, 此时使用如下命令观察 kafka 的 topics

```sh
docker exec flink-sql-docker-demo-kafka-1 kafka-topics.sh --zookeeper zookeeper:2181 --list
__consumer_offsets
user_behavior
```
这说明我们可以成功地在 docker 外面执行 kafka 相关指令来观察 kafka 的运行情况。


