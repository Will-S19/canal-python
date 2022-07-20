# Canal

[官网](https://github.com/alibaba/canal)
Mysql[主从搭建](http://liuqingzheng.top/python/%E5%85%B6%E4%BB%96/02-MySQL%E4%B8%BB%E4%BB%8E%E6%90%AD%E5%BB%BA%E5%9F%BA%E4%BA%8Edocker/)

# 环境准备

## 存储券

```bash
#在home目录下创建mysql文件夹，下面创建data和conf.d文件夹
mkdir /home/mysql && mkdir /home/mysql/conf.d && mkdir /home/mysql/data/
 
创建my.cnf配置文件
vim /home/mysql/my.cnf

[mysqld]
user=mysql
character-set-server=utf8
default_authentication_plugin=mysql_native_password
secure_file_priv=/var/lib/mysql
expire_logs_days=7
sql_mode=STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION
max_connections=1000
##主库----start--- 同一局域网内注意要唯一
server-id=100  
## 开启二进制日志功能，可以随便取（关键）
log-bin=mysql-bin
##主库----end--- 

##从库----start--- 
## 设置server_id,注意要唯一
server-id=101  
## 开启二进制日志功能，以备Slave作为其它Slave的Master时使用
log-bin=mysql-slave-bin   
## relay_log配置中继日志
relay_log=edu-mysql-relay-bin 
##从库----end--- 

[client]
default-character-set=utf8

[mysql]
default-character-set=utf8
```

## mysql容器准备

```bash
docker run \
-v /home/mysql/data/:/var/lib/mysql \
-v /home/mysql/conf.d:/etc/mysql/conf.d \
-v /home/mysql/my.cnf:/etc/mysql/my.cnf \
-p 33307:3306 \
--name mysql \
-e MYSQL_ROOT_PASSWORD=123456 -d mysql:5.7

查看地址
docker inspect mysql|grep -i ipaddress
172.17.0.2
```

连接进去后准备一个测试库

```sql
CREATE database canal;

-- 新建用户
CREATE USER canal IDENTIFIED BY 'canal';  
 
-- GRANT ALL PRIVILEGES ON *.* TO 'canal'@'%' ;
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
 
-- 刷新权限
FLUSH PRIVILEGES;
```

## rabbitmq容器准备

```bash
docker run -di --name rabbitmq -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=littlerabbit -p 15672:15672 -p 5672:5672 rabbitmq:management


docker ps|grep -i address
172.17.0.3
```

查看管理界面

![image-20220718224820151](C:\Users\S\AppData\Roaming\Typora\typora-user-images\image-20220718224820151.png)

# canal安装

[docker版本](https://github.com/alibaba/canal/wiki/Docker-QuickStart)

```bash
server-admin版本
canal-admin设计上是为canal提供整体配置管理、节点运维等面向运维的功能，提供相对友好的WebUI操作界面，方便更多用户快速和安全的操作

wget https://raw.githubusercontent.com/alibaba/canal/master/docker/run_admin.sh

# 指定外部的mysql作为admin的库
sh run_admin.sh -e server.port=8089 \
    -e spring.datasource.address=172.17.0.2:33306 \
    -e spring.datasource.database=canal \
    -e spring.datasource.username=canal \
    -e spring.datasource.password=canal



server
# 下载脚本
wget https://raw.githubusercontent.com/alibaba/canal/master/docker/run.sh 
这里会自动安装


sh run.sh -e canal.auto.scan=false \
		  -e canal.instance.master.address=127.0.0.1:8089  \
		  -e canal.instance.dbUsername=canal  \
		  -e canal.instance.dbPassword=canal  \
		  -e canal.instance.connectionCharset=UTF-8 \
		  -e canal.instance.tsdb.enable=true \
		  -e canal.instance.gtidon=false  \

mkdir -p /mydata/canal/conf && cd /mydata/canal/conf

docker cp canal-server:/home/admin/canal-server/conf/canal.properties /mydata/canal/conf
docker cp canal-server:/home/admin/canal-server/conf/test/instance.properties /mydata/canal/conf

docker stop canal-server && docker rm canal-server

```

## 分别是这两个文件

### canal.properties

```bash


#################################################
######### 		common argument		#############
#################################################
# tcp bind ip
canal.ip =
# register ip to zookeeper
canal.register.ip =
canal.port = 11111
canal.metrics.pull.port = 11112
# canal instance user/passwd
# canal.user = canal
# canal.passwd = E3619321C1A937C46A0D8BD1DAC39F93B27D4458

# canal admin config
#canal.admin.manager = 127.0.0.1:8089
canal.admin.port = 11110
canal.admin.user = admin
canal.admin.passwd = 4ACFE3202A5FF5CF467898FC58AAB1D615029441
# admin auto register
#canal.admin.register.auto = true
#canal.admin.register.cluster =
#canal.admin.register.name =

canal.zkServers =
# flush data to zk
canal.zookeeper.flush.period = 1000
canal.withoutNetty = false
# tcp, kafka, rocketMQ, rabbitMQ, pulsarMQ
canal.serverMode = rabbitMQ
# flush meta cursor/parse position to file
canal.file.data.dir = ${canal.conf.dir}
canal.file.flush.period = 1000
## memory store RingBuffer size, should be Math.pow(2,n)
canal.instance.memory.buffer.size = 16384
## memory store RingBuffer used memory unit size , default 1kb
canal.instance.memory.buffer.memunit = 1024 
## meory store gets mode used MEMSIZE or ITEMSIZE
canal.instance.memory.batch.mode = MEMSIZE
canal.instance.memory.rawEntry = true

## detecing config
canal.instance.detecting.enable = false
#canal.instance.detecting.sql = insert into retl.xdual values(1,now()) on duplicate key update x=now()
canal.instance.detecting.sql = select 1
canal.instance.detecting.interval.time = 3
canal.instance.detecting.retry.threshold = 3
canal.instance.detecting.heartbeatHaEnable = false

# support maximum transaction size, more than the size of the transaction will be cut into multiple transactions delivery
canal.instance.transaction.size =  1024
# mysql fallback connected to new master should fallback times
canal.instance.fallbackIntervalInSeconds = 60

# network config
canal.instance.network.receiveBufferSize = 16384
canal.instance.network.sendBufferSize = 16384
canal.instance.network.soTimeout = 30

# binlog filter config
canal.instance.filter.druid.ddl = true
canal.instance.filter.query.dcl = false
canal.instance.filter.query.dml = false
canal.instance.filter.query.ddl = false
canal.instance.filter.table.error = false
canal.instance.filter.rows = false
canal.instance.filter.transaction.entry = false
canal.instance.filter.dml.insert = false
canal.instance.filter.dml.update = false
canal.instance.filter.dml.delete = false

# binlog format/image check
canal.instance.binlog.format = ROW,STATEMENT,MIXED 
canal.instance.binlog.image = FULL,MINIMAL,NOBLOB

# binlog ddl isolation
canal.instance.get.ddl.isolation = false

# parallel parser config
canal.instance.parser.parallel = true
## concurrent thread number, default 60% available processors, suggest not to exceed Runtime.getRuntime().availableProcessors()
#canal.instance.parser.parallelThreadSize = 16
## disruptor ringbuffer size, must be power of 2
canal.instance.parser.parallelBufferSize = 256

# table meta tsdb info
canal.instance.tsdb.enable = true
canal.instance.tsdb.dir = ${canal.file.data.dir:../conf}/${canal.instance.destination:}
canal.instance.tsdb.url = jdbc:h2:${canal.instance.tsdb.dir}/h2;CACHE_SIZE=1000;MODE=MYSQL;
canal.instance.tsdb.dbUsername = canal
canal.instance.tsdb.dbPassword = canal
# dump snapshot interval, default 24 hour
canal.instance.tsdb.snapshot.interval = 24
# purge snapshot expire , default 360 hour(15 days)
canal.instance.tsdb.snapshot.expire = 360

#################################################
######### 		destinations		#############
#################################################
canal.destinations = example
# conf root dir
canal.conf.dir = ../conf
# auto scan instance dir add/remove and start/stop instance
canal.auto.scan = true
canal.auto.scan.interval = 5
# set this value to 'true' means that when binlog pos not found, skip to latest.
# WARN: pls keep 'false' in production env, or if you know what you want.
canal.auto.reset.latest.pos.mode = false

canal.instance.tsdb.spring.xml = classpath:spring/tsdb/h2-tsdb.xml
#canal.instance.tsdb.spring.xml = classpath:spring/tsdb/mysql-tsdb.xml

canal.instance.global.mode = spring
canal.instance.global.lazy = false
canal.instance.global.manager.address = ${canal.admin.manager}
#canal.instance.global.spring.xml = classpath:spring/memory-instance.xml
canal.instance.global.spring.xml = classpath:spring/file-instance.xml
#canal.instance.global.spring.xml = classpath:spring/default-instance.xml

##################################################
######### 	      MQ Properties      #############
##################################################
# aliyun ak/sk , support rds/mq
canal.aliyun.accessKey =
canal.aliyun.secretKey =
canal.aliyun.uid=

canal.mq.flatMessage = true
canal.mq.canalBatchSize = 50
canal.mq.canalGetTimeout = 100
# Set this value to "cloud", if you want open message trace feature in aliyun.
canal.mq.accessChannel = local

canal.mq.database.hash = true
canal.mq.send.thread.size = 30
canal.mq.build.thread.size = 8

##################################################
######### 		     Kafka 		     #############
##################################################
kafka.bootstrap.servers = 127.0.0.1:9092
kafka.acks = all
kafka.compression.type = none
kafka.batch.size = 16384
kafka.linger.ms = 1
kafka.max.request.size = 1048576
kafka.buffer.memory = 33554432
kafka.max.in.flight.requests.per.connection = 1
kafka.retries = 0

kafka.kerberos.enable = false
kafka.kerberos.krb5.file = "../conf/kerberos/krb5.conf"
kafka.kerberos.jaas.file = "../conf/kerberos/jaas.conf"

##################################################
######### 		    RocketMQ	     #############
##################################################
rocketmq.producer.group = test
rocketmq.enable.message.trace = false
rocketmq.customized.trace.topic =
rocketmq.namespace =
rocketmq.namesrv.addr = 127.0.0.1:9876
rocketmq.retry.times.when.send.failed = 0
rocketmq.vip.channel.enabled = false
rocketmq.tag = 

##################################################
######### 		    RabbitMQ	     #############
##################################################
rabbitmq.host = 172.17.0.3 # docker 网络地址  # 目前只支持默认端口
rabbitmq.virtual.host = /  # 默认vhost
rabbitmq.exchange = exchange.fanout.canal # exchange的名字，需要提前新建好
rabbitmq.username = admin
rabbitmq.password =littlerabbit
rabbitmq.deliveryMode =


##################################################
######### 		      Pulsar         #############
##################################################
pulsarmq.serverUrl =
pulsarmq.roleToken =
pulsarmq.topicTenantPrefix =
```

### instance.properties

```bash
#################################################
## mysql serverId , v1.0.26+ will autoGen
# canal.instance.mysql.slaveId=0

# enable gtid use true/false
canal.instance.gtidon=false

# position info
canal.instance.master.address=172.17.0.2:3306
canal.instance.master.journal.name=
canal.instance.master.position=
canal.instance.master.timestamp=
canal.instance.master.gtid=

# rds oss binlog
canal.instance.rds.accesskey=
canal.instance.rds.secretkey=
canal.instance.rds.instanceId=

# table meta tsdb info
canal.instance.tsdb.enable=true
#canal.instance.tsdb.url=jdbc:mysql://127.0.0.1:3306/canal_tsdb
#canal.instance.tsdb.dbUsername=canal
#canal.instance.tsdb.dbPassword=canal

#canal.instance.standby.address =
#canal.instance.standby.journal.name =
#canal.instance.standby.position =
#canal.instance.standby.timestamp =
#canal.instance.standby.gtid=

# username/password
canal.instance.dbUsername=canal
canal.instance.dbPassword=canal
canal.instance.connectionCharset = UTF-8
# enable druid Decrypt database password
canal.instance.enableDruid=false
#canal.instance.pwdPublicKey=MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBALK4BUxdDltRRE5/zXpVEVPUgunvscYFtEip3pmLlhrWpacX7y7GCMo2/JM6LeHmiiNdH1FWgGCpUfircSwlWKUCAwEAAQ==

# table regex
canal.instance.filter.regex=.*\\..*
# table black regex
canal.instance.filter.black.regex=mysql\\.slave_.*
# table field filter(format: schema1.tableName1:field1/field2,schema2.tableName2:field1/field2)
#canal.instance.filter.field=test1.t_product:id/subject/keywords,test2.t_company:id/name/contact/ch
# table field black filter(format: schema1.tableName1:field1/field2,schema2.tableName2:field1/field2)
#canal.instance.filter.black.field=test1.t_product:subject/product_image,test2.t_company:id/name/contact/ch

# mq config
canal.mq.topic=canal-routing-key
# dynamic topic route by schema or table regex
#canal.mq.dynamicTopic=mytest1.user,topic2:mytest2\\..*,.*\\..*
canal.mq.partition=0
# hash partition config
#canal.mq.enableDynamicQueuePartition=false
#canal.mq.partitionsNum=3
#canal.mq.dynamicTopicPartitionNum=test.*:4,mycanal:6
#canal.mq.partitionHash=test.table:id^name,.*\\..*
#################################################

```



## 重新启动

```bash
docker run -d \
-p 11111:11111 -v /mydata/canal/conf/canal.properties:/home/admin/canal-server/conf/canal.properties \
-v /mydata/canal/conf/instance.properties:/home/admin/canal-server/conf/test/instance.properties \
-v /mydata/canal/log:/home/admin/canal-server/logs/ \
--name=canal-server canal/canal-server
```

