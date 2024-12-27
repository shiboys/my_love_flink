# ClickHouse 安装与部署

## 安装环境选择

我想在我本机安装 clickhouse ，以便进行学习，但是我查了很多资料，都没成功，最后还是参考官方的文档搞定了。
1. mac 本机安装，我本来打算像 Flink 一样，安装 Mac 版本的，但是 官方建议通过源码编译安装， 然后要下载 XCode，我天，我磁盘空间就剩 10+ G 了， XCode 根本安装不上，放弃！
2. 想像 mysql5.7 一样，通过 docker 安装，不就是一个数据库吗？docker 上拉一个，结果 docker 完全连不上，配了国内的镜像也不好使，我估计是需要 signin ，但是 docker hub 根本连不上，也无法 sign in
3. 虚拟机安装，parallels desktop 因为 macOS 的升级，之前的 linux 虚拟机打不开了，要打开我查了下，步骤非常繁琐，我一气之下直接把这个 虚拟机和 parallels desktop 给删除了，换上了 Vmware 的 Fusion ，而且 VmWare 对于个人学习和使用完全免费，Vmware 没有 parallels desktop 那样自带 Linux ，我又从网上下载了个 Centos8.iso ，10+G, 磁盘空间不够使呀，头疼。。。加购的 Mac 系统可以读写的联想 U 盘在路上了。然后虚拟机就安装成功了，再然后我就按照官方文档，打算尝试使用 clickhouse rpm 包进行安装，官方给了一个安装的 shell 脚本，如下所示：

```sh
LATEST_VERSION=$(curl -s https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/utils/list-versions/version_date.tsv | \
    grep -Eo '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | sort -V -r | head -n 1)
export LATEST_VERSION

case $(uname -m) in
  x86_64) ARCH=amd64 ;;
  aarch64) ARCH=arm64 ;;
  *) echo "Unknown architecture $(uname -m)"; exit 1 ;;
esac

for PKG in clickhouse-common-static clickhouse-common-static-dbg clickhouse-server clickhouse-client clickhouse-keeper
do
  curl -fO "https://packages.clickhouse.com/tgz/stable/$PKG-$LATEST_VERSION-${ARCH}.tgz" \
    || curl -fO "https://packages.clickhouse.com/tgz/stable/$PKG-$LATEST_VERSION.tgz"
done

tar -xzvf "clickhouse-common-static-$LATEST_VERSION-${ARCH}.tgz" \
  || tar -xzvf "clickhouse-common-static-$LATEST_VERSION.tgz"
sudo "clickhouse-common-static-$LATEST_VERSION/install/doinst.sh"

tar -xzvf "clickhouse-common-static-dbg-$LATEST_VERSION-${ARCH}.tgz" \
  || tar -xzvf "clickhouse-common-static-dbg-$LATEST_VERSION.tgz"
sudo "clickhouse-common-static-dbg-$LATEST_VERSION/install/doinst.sh"

tar -xzvf "clickhouse-server-$LATEST_VERSION-${ARCH}.tgz" \
  || tar -xzvf "clickhouse-server-$LATEST_VERSION.tgz"
sudo "clickhouse-server-$LATEST_VERSION/install/doinst.sh" configure
sudo /etc/init.d/clickhouse-server start

tar -xzvf "clickhouse-client-$LATEST_VERSION-${ARCH}.tgz" \
  || tar -xzvf "clickhouse-client-$LATEST_VERSION.tgz"
sudo "clickhouse-client-$LATEST_VERSION/install/doinst.sh"
```

仔细读下这个脚本，使用 github 上读取最新的版本，然后解压安装，github 又访问不了，这。。。又进入死胡同了。。。
没办法，找镜像吧，发现了阿里云的 clickhouse 镜像，牛逼大发了，居然安装成功了。具体怎么弄得，步骤如下：
1. 把上面的脚本的版本以及安装平台，使用常量给修改了，修改如下
```sh
LATEST_VERSION='24.9.3.128'

ARCH='amd64'

for PKG in clickhouse-common-static clickhouse-common-static-dbg clickhouse-server clickhouse-client clickhouse-keeper
do
  curl -fO "https://mirrors.aliyun.com/clickhouse/tgz/stable/$PKG-$LATEST_VERSION-${ARCH}.tgz" \
    || curl -fO "https://mirrors.aliyun.com/clickhouse/tgz/stable/$PKG-$LATEST_VERSION.tgz"
done

tar -xzvf "clickhouse-common-static-$LATEST_VERSION-${ARCH}.tgz" \
  || tar -xzvf "clickhouse-common-static-$LATEST_VERSION.tgz"
sudo "clickhouse-common-static-$LATEST_VERSION/install/doinst.sh"

tar -xzvf "clickhouse-common-static-dbg-$LATEST_VERSION-${ARCH}.tgz" \
  || tar -xzvf "clickhouse-common-static-dbg-$LATEST_VERSION.tgz"
sudo "clickhouse-common-static-dbg-$LATEST_VERSION/install/doinst.sh"

tar -xzvf "clickhouse-server-$LATEST_VERSION-${ARCH}.tgz" \
  || tar -xzvf "clickhouse-server-$LATEST_VERSION.tgz"
sudo "clickhouse-server-$LATEST_VERSION/install/doinst.sh" configure
sudo /etc/init.d/clickhouse-server start

tar -xzvf "clickhouse-client-$LATEST_VERSION-${ARCH}.tgz" \
  || tar -xzvf "clickhouse-client-$LATEST_VERSION.tgz"
sudo "clickhouse-client-$LATEST_VERSION/install/doinst.sh"
```
2. 使用 scp 命令传到 虚拟机上
3. 在虚拟机上运行这个脚本，成功安装了 ClickHouse-Server 和 ClickHouse-Client
4. 运行 ClickHouse，然后就一堆错误，主要是权限不足，按照网上的描述，将 clickhouse 的目录，比如 /var/data/clickhouse 等进行 change owner, 如下语句
   ```sh
   chown -R clickhouse:clickhouse /var/lib/clickhouse /var/log/clickhouse-server/ /etc/clickhouse-server/ /etc/clickhouse-client/
   ```
   改完之后，还是报错，查资料说，要给 clickhouse:clickhouse 赋予 root 权限，搞
   ```sh
   usermod -a -G root clickhouse
   ```
   搞完之后，还是运行报错，
   ```
   Code: 76. DB::ErrnoException: Cannot open file /usr/lib/debug/usr/bin/clickhouse.debug: , errno: 13, strerror: Permission denied. (CANNOT_OPEN_FILE), Stack trace (when copying this message, always include the lines below)
   ```
   后来仔细一读是这文件的问题，把之前更改的 root:root 重新改为 clickhouse:clickhouse, 反正现在 clickhouse 也有 root 权限了
   然后把这个报错的文件给 rm 了
   然后重新运行
   ```sh
   service clickhouse-server start
   ```
   然后，奇迹就出现了，clickhouse 成功地运行起来了。。。。牛逼。。。。散花。。。。
5. 既然 clickhouse 已经运行起来，那就是用 clickhouse-client 连上 server ，sql 命令搞起来把。。。
   然后，我又过于乐观了，click house 默认的用户名是 default ，我不知道密码，这怎么办，继续 google 吧。
   最后发现 clickhouse 的默认用户在 , 打开 /etc/clickhouse-server/user.xml, 发现用户的密码配置说明如下：
   ```xml
   <users>
        <!-- If user name was not specified, 'default' user is used. -->
        <default>
            <!-- See also the files in users.d directory where the password can be overridden.

                 Password could be specified in plaintext or in SHA256 (in hex format).

                 If you want to specify password in plaintext (not recommended), place it in 'password' element.
                 Example: <password>qwerty</password>.
                 Password could be empty.

                 If you want to specify SHA256, place it in 'password_sha256_hex' element.
                 Example: <password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>
                 Restrictions of SHA256: impossibility to connect to ClickHouse using MySQL JS client (as of July 2019).

                 If you want to specify double SHA1, place it in 'password_double_sha1_hex' element.
                 Example: <password_double_sha1_hex>e395796d6546b1b65db9d665cd43f0e858dd4303</password_double_sha1_hex>

                 If you want to specify a previously defined LDAP server (see 'ldap_servers' in the main config) for authentication,
                  place its name in 'server' element inside 'ldap' element.
                 Example: <ldap><server>my_ldap_server</server></ldap>

                ...
            -->
                  <password>123456</password>
        </default>
    </users>
   ```
我刚开始觉得 123456， 我就是用 sha256 的方式把我自己的密码进行加密，然后配置为<password_sha256_hex>xxx</password_sha256_hex>
重启 clickhouse-server ，使用 client 登录，验证失败
然后我把这里改用明文的 123456，然后使用 service clickhouse-server restart ，重启很快，但是依然连接报验证失败

```text
[root@centos clickhouse-server]# clickhouse-client
ClickHouse client version 24.9.2.42 (official build).
Connecting to localhost:9000 as user default.
Password for user (default):
Connecting to localhost:9000 as user default.
Code: 516. DB::Exception: Received from localhost:9000. DB::Exception: default: Authentication failed: password is incorrect, or there is no user with such name.

If you have installed ClickHouse and forgot password you can reset it in the configuration file.
The password for default user is typically located at /etc/clickhouse-server/users.d/default-password.xml
and deleting this file will reset the password.
See also /etc/clickhouse-server/users.xml on the server where ClickHouse is installed.

. (AUTHENTICATION_FAILED)
```

最后我仔细读了下这个验证异常的信息，发现 default 用户的密码可能存在 /etc/clickhouse-server/users.d/default-password.xml
我直接 vim 了这个文件，如下所示
```xml
<clickhouse>
    <users>
        <default>
            <password remove='1' />
            <password_sha256_hex>8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92</password_sha256_hex>
        </default>
    </users>
</clickhouse>
```

卧槽，市外桃园有木有，应该是这个不错了，使用命令 
```sh
echo -n 123456 | openssl dgst -sha256
```
生成 sha256 的 16 进制加密密码字符串，然后修改保存，重启 clickhouse-server, 使用客户端重连。。。。天哪，连上了，牛逼大发了。。。

```sql
[root@centos clickhouse-server]# clickhouse-client
ClickHouse client version 24.9.2.42 (official build).
Connecting to localhost:9000 as user default.
Password for user (default):
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 24.9.2.

Warnings:
 * Linux transparent hugepages are set to "always". Check /sys/kernel/mm/transparent_hugepage/enabled
 * Linux threads max count is too low. Check /proc/sys/kernel/threads-max
 * Maximum number of threads is lower than 30000. There could be problems with handling a lot of simultaneous queries.

centos.swj.com :) show databases;

SHOW DATABASES

Query id: 63016789-6d97-4cb5-8c27-45f688663dea

   ┌─name───────────────┐
1. │ INFORMATION_SCHEMA │
2. │ default            │
3. │ information_schema │
4. │ system             │
   └────────────────────┘

4 rows in set. Elapsed: 0.003 sec.

centos.swj.com :) user default;
```

至此，我的 clickhouse 总算安装成功了。

下面就是开始使用 clickhouse 进行入门课程的学习了。

## 目录结构

### 核心目录

1. /etc/clickhouse-server：服务端的配置文件目录，包括全局配置 config.xml 和用户配置 users.xml
2. /var/lib/clickhouse: 默认的数据存储目录(通常会修改默认路径配置，将数据保存到大容量磁盘挂载的路径)
3. /var/log/clickhouse-server: 默认保存日志的目录(通常会修改默认路径配置，将日志保存到大容量磁盘挂载的路径)

### 配置文件
1. /etc/security/limits.d/clickhouse.conf, 文件句柄数量配置

```sh
[root@centos ~]# cat /etc/security/limits.d/clickhouse.conf
clickhouse	soft	nofile	1048576
clickhouse	hard	nofile	1048576
```
该配置也可以通过 config.xml 的 max_open_files 修改

### 主程序说明

/usr/bin 下的可执行文件:
1. clickhouse：主程序的可执行文件
2. clickhouse-server 一个执行 ClickHouse 可执行文件的软连接 ，供服务端使用
3. clickhouse-client 一个执行 ClickHouse 可执行文件的软连接 ，供客户端使用
4. clickhouse-compressor: 内置的压缩工具，可用于数据的正反解压。

## CLI

CLI(Command Line Interface) 即命令行接口，其底层是基于 TCP 接口进行通信的，是通过 clickhouse-client 脚本运行的。它具有两种模式：
交互模式 和 非交互模式。交互模式就是命令行模式, 如下所示 
```sql
centos.swj.com :) select bar(number,0,4) from numbers(4);

SELECT bar(number, 0, 4)
FROM numbers(4)

Query id: 1cae3477-a524-4395-aff6-cb2951671ec3

   ┌─bar(number, 0, 4)────────────────────────────────────────────┐
1. │                                                              │
2. │ ████████████████████                                         │
3. │ ████████████████████████████████████████                     │
4. │ ████████████████████████████████████████████████████████████ │
   └──────────────────────────────────────────────────────────────┘

4 rows in set. Elapsed: 0.011 sec.
```

### 交互式 Sql

#### 书中和官方示例尝试

通过交互式执行的 SQL 语句，相关查询结果会统一记录到 ~/.clickhouse-client-history 文件，该文件可以作为审计之用

```sh
less ~/.clickhouse-client-history
### 2024-12-09 04:43:08.267
centos.swj.com :) select bar(number,0,4) from numbers(4);

SELECT bar(number, 0, 4)
FROM numbers(4)

Query id: 1cae3477-a524-4395-aff6-cb2951671ec3

   ┌─bar(number, 0, 4)────────────────────────────────────────────┐
1. │                                                              │
2. │ ████████████████████                                         │
3. │ ████████████████████████████████████████                     │
4. │ ████████████████████████████████████████████████████████████ │
   └──────────────────────────────────────────────────────────────┘
```

```sql
CREATE TABLE hackernews (
    id UInt32,
    type String,
    author String,
    timestamp DateTime,
    comment String,
    children Array(UInt32),
    tokens Array(String)
)
ENGINE = MergeTree
ORDER BY toYYYYMMDD(timestamp)
```

### 非交互式 Sql

参考官方文档 https://clickhouse.ac.cn/docs/en/integrations/data-ingestion/insert-local-files#google_vignette

将 tsv 文件拷贝下来，指向如下非交互式 sql
```sh
clickhouse-client     --host localhost     --port 9000     --password 123456     --query "
    INSERT INTO hackernews
    SELECT
        id,
        type,
        lower(author),
        timestamp,
        comment,
        children,
        extractAll(comment, '\\w+') as tokens
    FROM input('id UInt32, type String, author String, timestamp DateTime, comment String, children Array(UInt32)')
    FORMAT TabSeparatedWithNames
" < comments.tsv
```

这样，数据就已上传到 clickhouse

```sql
centos.swj.com :) select * from hackernews limit 7;

SELECT *
FROM hackernews
LIMIT 7

Query id: 54c05dd6-c6b5-4d74-9fe9-2d0122439d37

   ┌─id─┬─type─┬─author─┬───────────timestamp─┬─comment─┬─children─┬─tokens─┐
1. │  0 │      │        │ 1969-12-31 16:00:00 │         │ []       │ []     │
2. │  0 │      │        │ 1969-12-31 16:00:00 │         │ []       │ []     │
3. │  0 │      │        │ 1969-12-31 16:00:00 │         │ []       │ []     │
4. │  0 │      │        │ 1969-12-31 16:00:00 │         │ []       │ []     │
5. │  0 │      │        │ 1969-12-31 16:00:00 │         │ []       │ []     │
6. │  0 │      │        │ 1969-12-31 16:00:00 │         │ []       │ []     │
7. │  0 │      │        │ 1969-12-31 16:00:00 │         │ []       │ []     │
   └────┴──────┴────────┴─────────────────────┴─────────┴──────────┴────────┘

7 rows in set. Elapsed: 0.042 sec.
```

之所以会出现上述这种情况，是因为我的 tsv 从官方网站上拷贝下来，\t 这个间距符号不正确，但是一般的编辑器也显示不出来，这怎么办？我一度怀疑是我用的 centos8 版本太高或者  clickhouse 24.9 的安装包太新了，怎么排查，当然是从易到难了。
1. 我先从简单入手，先使用最简单的一行 csv 开始，然后字符串也特别少，成功插入
2. 我换成 tsv 后来发现无法写入，经过使用 sublime Text 发现制表符 \t 不是特别明显，就用制表符重新将各个字段隔开，重新插入 OK。
3. 我使用带有 title 的 tsv ，format 使用官方示例的 TabSeparatedWithNames 插入，也 OK
4. 这就是问题了，我把从官方拷贝下来的字符，全部在 sublime text 用制表符 \t 重新编辑下，拷贝到 tsv 文件中，保存、重新提交，插入 OK

经过重新编辑之后的结果如下：

```sql
centos.swj.com :) select id, type, author,timestamp, substring(comment,1,10) as comment, children, arraySlice(tokens,1,3) as tokens  from hackernews;

SELECT
    id,
    type,
    author,
    timestamp,
    substring(comment, 1, 10) AS comment,
    children,
    arraySlice(tokens, 1, 3) AS tokens
FROM hackernews

Query id: 0c69e3ed-24a6-4087-aaa4-c789e788a0f0

   ┌───────id─┬─type────┬─author──────────┬───────────timestamp─┬─comment────┬─children───┬─tokens──────────────────────┐
1. │ 19464423 │ comment │ adrianmonk      │ 2019-03-22 16:58:19 │ "It&#x27;s │ []         │ ['It','x27','s']            │
2. │ 19464461 │ comment │ neakernets      │ 2019-03-22 17:01:10 │ "Because t │ [19464582] │ ['Because','the','science'] │
3. │ 19465288 │ comment │ derefr          │ 2019-03-22 18:15:21 │ "Because w │ []         │ ['Because','we','x27']      │
4. │ 19465534 │ comment │ bduerst         │ 2019-03-22 18:36:40 │ "Apple inc │ []         │ ['Apple','included','a']    │
5. │ 19466269 │ comment │ calchris        │ 2019-03-22 19:55:13 │ "&gt; It h │ [19468341] │ ['gt','It','has']           │
6. │ 19466980 │ comment │ onetimemanytime │ 2019-03-22 21:07:25 │ "&gt;&gt;< │ []         │ ['gt','gt','i']             │
7. │ 19467048 │ comment │ karambahh       │ 2019-03-22 21:15:41 │ "I think y │ [19467512] │ ['I','think','you']         │
   └──────────┴─────────┴─────────────────┴─────────────────────┴────────────┴────────────┴─────────────────────────────┘

7 rows in set. Elapsed: 0.005 sec.
```


```sql
 create table test(id UInt32, desc String, timestamp Datetime) ENGINE=MergeTree order by id;
```
```sh
cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
cat <<_EOF | clickhouse-client --host localhost --port 9000 --password 123456 --database=default --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF
```

#### 官方 Sql 说明

`**lower** 函数` 可以将列转换成小写。我们还希望将 `comment` 字符串拆分为标记并将结果存储在 `tokens` 列中，可以使用 `extractAll` 函数完成。

最重要的是 `input` 函数, 该函数在这里也很有用，因为它允许我们在将数据插入 `hackernews` 表时对其进行转换。 `input` 的参数是传入原始数据的格式，我们会在许多其他函数中看到这一点(需要为其传入数据指定模式)

## 内置使用工具

### ClickHouse-local

clickhouse-local  可以独立运行大部分 Sql 查询，不需要依赖任何 Clickhouse 的服务端程序，他开业理解成 ClickHouse 的单机版微内核，是一个轻量级的应用程序。Clickhouse-local 只能使用 File 表引擎，它的数据与运行的 ClickHouse 服务也是完全隔离的，相互之间不能访问。

```sh
ps aux | tail -n +2 |  awk '{printf("%s\t%s\n",$1,$4)}' | clickhouse-local -S "user String, memory Float64" -q "select user,round(sum(memory),2) as memoryTotal from table Group By user ORDER BY memoryTotal DESC FORMAT Pretty";
```
结果如下
```sql
    ┏━━━━━━━━━━┳━━━━━━━━━━━━━┓
    ┃ user     ┃ memoryTotal ┃
    ┡━━━━━━━━━━╇━━━━━━━━━━━━━┩
 1. │ clickho+ │        36.7 │
    ├──────────┼─────────────┤
 2. │ gdm      │        22.7 │
    ├──────────┼─────────────┤
 3. │ root     │        20.8 │
    ├──────────┼─────────────┤
 4. │ polkitd  │           1 │
    ├──────────┼─────────────┤
 5. │ colord   │         0.4 │
    ├──────────┼─────────────┤
 6. │ dbus     │         0.2 │
    ├──────────┼─────────────┤
 7. │ rtkit    │         0.1 │
    ├──────────┼─────────────┤
 8. │ chrony   │         0.1 │
    ├──────────┼─────────────┤
 9. │ rpc      │         0.1 │
    ├──────────┼─────────────┤
10. │ avahi    │         0.1 │
    ├──────────┼─────────────┤
11. │ libstor+ │           0 │
    ├──────────┼─────────────┤
12. │ dnsmasq  │           0 │
    └──────────┴─────────────┘
```

参数解释如下：
```text
-S [ --structure ] arg                  structure of the initial table (list of column and type names)
-N/--table 表明，默认值是 table
-if/ --input-format 输入数据的格式，默认是 TSV
-q/--query, 待执行的 Sql 语句
```

### clickhouse-benchmark

```sh
echo "select * from system.numbers limit 100" | clickhouse-benchmark -i 5 --host localhost     --port 9000     --password 123456
```
输出如下，输出结果包括 qps, rps 等
```sql
Loaded 1 queries.

Queries executed: 5.

localhost:9000, queries: 5, QPS: 26.940, RPS: 2694.030, MiB/s: 0.021, result RPS: 2694.030, result MiB/s: 0.021.

0.000%		0.003 sec.
10.000%		0.003 sec.
20.000%		0.003 sec.
30.000%		0.003 sec.
40.000%		0.003 sec.
50.000%		0.003 sec.
60.000%		0.003 sec.
70.000%		0.004 sec.
80.000%		0.004 sec.
90.000%		0.021 sec.
95.000%		0.021 sec.
99.000%		0.021 sec.
99.900%		0.021 sec.
99.990%		0.021 sec.
```