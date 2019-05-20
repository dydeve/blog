---
title: spark shell
my: spark-shell
date: 2019-05-18 15:43:05
 - spark
 - big data
categories: 
 - spark
 - The Art of Spark Kernel Design
---

安装spark的流程就不说了。本篇讲述spark-shell

### log level
在$SPARK_HOME/conf目录中，有log4j.properties文件(如果没有 执行`cp log4j.properties.template log4j.properties`). spark-shell默认日志级别为WARN,修改log4j.properties如下，以打印更详细的信息
```text
# Set the default spark-shell log level to WARN. When running the spark-shell, the
# log level for this class is used to overwrite the root logger's log level, so that
# the user can have different defaults for the shell and regular Spark apps.
log4j.logger.org.apache.spark.repl.Main=INFO
```

### run workCount on spark-shell
执行$SPARK_HOME/bin/spark-shell，结果如下
```console
(base) joker:spark xmly$ bin/spark-shell
19/05/18 15:39:47 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
19/05/18 15:39:47 INFO SignalUtils: Registered signal handler for INT
19/05/18 15:39:53 INFO SparkContext: Running Spark version 2.3.1
19/05/18 15:39:53 INFO SparkContext: Submitted application: Spark shell
19/05/18 15:39:53 INFO SecurityManager: Changing view acls to: xmly
19/05/18 15:39:53 INFO SecurityManager: Changing modify acls to: xmly
19/05/18 15:39:53 INFO SecurityManager: Changing view acls groups to:
19/05/18 15:39:53 INFO SecurityManager: Changing modify acls groups to:
19/05/18 15:39:53 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(xmly); groups with view permissions: Set(); users  with modify permissions: Set(xmly); groups with modify permissions: Set()
19/05/18 15:39:54 INFO Utils: Successfully started service 'sparkDriver' on port 57338.
19/05/18 15:39:54 INFO SparkEnv: Registering MapOutputTracker
19/05/18 15:39:54 INFO SparkEnv: Registering BlockManagerMaster
19/05/18 15:39:54 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
19/05/18 15:39:54 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
19/05/18 15:39:54 INFO DiskBlockManager: Created local directory at /private/var/folders/sb/7sj8qth16j71x82r2m6ctk8r0000gn/T/blockmgr-fcefbfe5-40a6-4058-9724-c3ee2c701190
19/05/18 15:39:54 INFO MemoryStore: MemoryStore started with capacity 366.3 MB
19/05/18 15:39:54 INFO SparkEnv: Registering OutputCommitCoordinator
19/05/18 15:39:54 INFO Utils: Successfully started service 'SparkUI' on port 4040.
19/05/18 15:39:54 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://192.168.38.95:4040
19/05/18 15:39:55 INFO Executor: Starting executor ID driver on host localhost
19/05/18 15:39:55 INFO Executor: Using REPL class URI: spark://192.168.38.95:57338/classes
19/05/18 15:39:55 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 57339.
19/05/18 15:39:55 INFO NettyBlockTransferService: Server created on 192.168.38.95:57339
19/05/18 15:39:55 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
19/05/18 15:39:55 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 192.168.38.95, 57339, None)
19/05/18 15:39:55 INFO BlockManagerMasterEndpoint: Registering block manager 192.168.38.95:57339 with 366.3 MB RAM, BlockManagerId(driver, 192.168.38.95, 57339, None)
19/05/18 15:39:55 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 192.168.38.95, 57339, None)
19/05/18 15:39:55 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 192.168.38.95, 57339, None)
19/05/18 15:39:56 INFO EventLoggingListener: Logging events to hdfs://localhost:9000/spark_log/local-1558165194992.lz4
19/05/18 15:39:56 INFO Main: Created Spark session with Hive support
Spark context Web UI available at http://192.168.38.95:4040
Spark context available as 'sc' (master = local[*], app id = local-1558165194992).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.3.1
      /_/

Using Scala version 2.11.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_171)
Type in expressions to have them evaluated.
Type :help for more information.

scala>
```

从启动日志，可看到SparkContext、SecurityManager、SparkEnv、BlockManagerMasterEndpoint、DiskBlockManager、MemoryStore、SparkUI、Executor、NettyBlockTransferService、BlockManager、等

以下执行workCount
```scala
scala> :paste
// Entering paste mode (ctrl-D to finish)

val lines = sc.textFile("file:///opt/spark/README.md")
val words = lines.flatMap(line => line.split(" "))
val ones = words.map(word => (word, 1))
val counts = ones.reduceByKey(_ + _)

// Exiting paste mode, now interpreting.

19/05/18 16:09:07 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 241.5 KB, free 366.1 MB)
19/05/18 16:09:07 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 23.3 KB, free 366.0 MB)
19/05/18 16:09:07 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 192.168.38.95:57718 (size: 23.3 KB, free: 366.3 MB)
19/05/18 16:09:07 INFO SparkContext: Created broadcast 0 from textFile at <console>:24
19/05/18 16:09:07 INFO FileInputFormat: Total input paths to process : 1
lines: org.apache.spark.rdd.RDD[String] = file:///opt/spark/README.md MapPartitionsRDD[1] at textFile at <console>:24
words: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[2] at flatMap at <console>:25
ones: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[3] at map at <console>:26
counts: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[4] at reduceByKey at <console>:27

scala>
```
可以看到reducyByKey算子将`MapPartitionsRDD`转变成`ShuffledRDD`

通过toDebugString可以看到更详细的RDD转换过程
```console
scala> counts.toDebugString
res3: String =
(2) ShuffledRDD[4] at reduceByKey at <console>:27 []
 +-(2) MapPartitionsRDD[3] at map at <console>:26 []
    |  MapPartitionsRDD[2] at flatMap at <console>:25 []
    |  file:///opt/spark/README.md MapPartitionsRDD[1] at textFile at <console>:24 []
    |  file:///opt/spark/README.md HadoopRDD[0] at textFile at <console>:24 []
```

执行action操作
```console
scala> counts.collect()
19/05/18 16:13:26 INFO SparkContext: Starting job: collect at <console>:26
19/05/18 16:13:26 INFO DAGScheduler: Registering RDD 3 (map at <console>:26)
19/05/18 16:13:26 INFO DAGScheduler: Got job 0 (collect at <console>:26) with 2 output partitions
19/05/18 16:13:26 INFO DAGScheduler: Final stage: ResultStage 1 (collect at <console>:26)
19/05/18 16:13:26 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 0)
19/05/18 16:13:26 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 0)
19/05/18 16:13:26 INFO DAGScheduler: Submitting ShuffleMapStage 0 (MapPartitionsRDD[3] at map at <console>:26), which has no missing parents
19/05/18 16:13:26 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 4.8 KB, free 366.0 MB)
19/05/18 16:13:26 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 2.8 KB, free 366.0 MB)
19/05/18 16:13:26 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 192.168.38.95:57718 (size: 2.8 KB, free: 366.3 MB)
19/05/18 16:13:26 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1039
19/05/18 16:13:26 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 0 (MapPartitionsRDD[3] at map at <console>:26) (first 15 tasks are for partitions Vector(0, 1))
19/05/18 16:13:26 INFO TaskSchedulerImpl: Adding task set 0.0 with 2 tasks
19/05/18 16:13:26 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, executor driver, partition 0, PROCESS_LOCAL, 7861 bytes)
19/05/18 16:13:26 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1, localhost, executor driver, partition 1, PROCESS_LOCAL, 7861 bytes)
19/05/18 16:13:26 INFO Executor: Running task 1.0 in stage 0.0 (TID 1)
19/05/18 16:13:26 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
19/05/18 16:13:26 INFO HadoopRDD: Input split: file:/opt/spark/README.md:0+1904
19/05/18 16:13:26 INFO HadoopRDD: Input split: file:/opt/spark/README.md:1904+1905
19/05/18 16:13:26 INFO Executor: Finished task 1.0 in stage 0.0 (TID 1). 1155 bytes result sent to driver
19/05/18 16:13:26 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 1155 bytes result sent to driver
19/05/18 16:13:26 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 202 ms on localhost (executor driver) (1/2)
19/05/18 16:13:26 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 223 ms on localhost (executor driver) (2/2)
19/05/18 16:13:26 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool
19/05/18 16:13:26 INFO DAGScheduler: ShuffleMapStage 0 (map at <console>:26) finished in 0.318 s
19/05/18 16:13:26 INFO DAGScheduler: looking for newly runnable stages
19/05/18 16:13:26 INFO DAGScheduler: running: Set()
19/05/18 16:13:26 INFO DAGScheduler: waiting: Set(ResultStage 1)
19/05/18 16:13:26 INFO DAGScheduler: failed: Set()
19/05/18 16:13:26 INFO DAGScheduler: Submitting ResultStage 1 (ShuffledRDD[4] at reduceByKey at <console>:27), which has no missing parents
19/05/18 16:13:26 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 3.2 KB, free 366.0 MB)
19/05/18 16:13:26 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 1978.0 B, free 366.0 MB)
19/05/18 16:13:26 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 192.168.38.95:57718 (size: 1978.0 B, free: 366.3 MB)
19/05/18 16:13:26 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1039
19/05/18 16:13:27 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 1 (ShuffledRDD[4] at reduceByKey at <console>:27) (first 15 tasks are for partitions Vector(0, 1))
19/05/18 16:13:27 INFO TaskSchedulerImpl: Adding task set 1.0 with 2 tasks
19/05/18 16:13:27 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 2, localhost, executor driver, partition 0, ANY, 7649 bytes)
19/05/18 16:13:27 INFO TaskSetManager: Starting task 1.0 in stage 1.0 (TID 3, localhost, executor driver, partition 1, ANY, 7649 bytes)
19/05/18 16:13:27 INFO Executor: Running task 0.0 in stage 1.0 (TID 2)
19/05/18 16:13:27 INFO Executor: Running task 1.0 in stage 1.0 (TID 3)
19/05/18 16:13:27 INFO ShuffleBlockFetcherIterator: Getting 2 non-empty blocks out of 2 blocks
19/05/18 16:13:27 INFO ShuffleBlockFetcherIterator: Getting 2 non-empty blocks out of 2 blocks
19/05/18 16:13:27 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 6 ms
19/05/18 16:13:27 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 7 ms
19/05/18 16:13:27 INFO Executor: Finished task 0.0 in stage 1.0 (TID 2). 4673 bytes result sent to driver
19/05/18 16:13:27 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 2) in 111 ms on localhost (executor driver) (1/2)
19/05/18 16:13:27 INFO Executor: Finished task 1.0 in stage 1.0 (TID 3). 4539 bytes result sent to driver
19/05/18 16:13:27 INFO TaskSetManager: Finished task 1.0 in stage 1.0 (TID 3) in 117 ms on localhost (executor driver) (2/2)
19/05/18 16:13:27 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool
19/05/18 16:13:27 INFO DAGScheduler: ResultStage 1 (collect at <console>:26) finished in 0.150 s
19/05/18 16:13:27 INFO DAGScheduler: Job 0 finished: collect at <console>:26, took 0.753798 s
res1: Array[(String, Int)] = Array((package,1), (this,1)...)
```

SparkContext开启job，id是0。

DAGScheduler划分、提交两个stage。第一个stage为ShuffleMapStage，id为0；第二个stage为ResultStage，id为1

每个stage都有两个task，因为`2 output partitions`(line 4)

TaskSchedulerImpl添加task到task set，Executor执行task

### analyse spark-shell

part of spark-shell
```shell
function main() {
  if $cygwin; then
    # Workaround for issue involving JLine and Cygwin
    # (see http://sourceforge.net/p/jline/bugs/40/).
    # If you're using the Mintty terminal emulator in Cygwin, may need to set the
    # "Backspace sends ^H" setting in "Keys" section of the Mintty options
    # (see https://github.com/sbt/sbt/issues/562).
    stty -icanon min 1 -echo > /dev/null 2>&1
    export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -Djline.terminal=unix"
    "${SPARK_HOME}"/bin/spark-submit --class org.apache.spark.repl.Main --name "Spark shell" "$@"
    stty icanon echo > /dev/null 2>&1
  else
    export SPARK_SUBMIT_OPTS
    "${SPARK_HOME}"/bin/spark-submit --class org.apache.spark.repl.Main --name "Spark shell" "$@"
  fi
}
```
spark-shell执行了`spark-submit`脚本

spark-submit
```shell
if [ -z "${SPARK_HOME}" ]; then
  source "$(dirname "$0")"/find-spark-home
fi

# disable randomized hash for string in Python 3.3+
export PYTHONHASHSEED=0

exec "${SPARK_HOME}"/bin/spark-class org.apache.spark.deploy.SparkSubmit "$@"
```
执行`spark-class`

part of spark-class
```scala
# Find the java binary
if [ -n "${JAVA_HOME}" ]; then
  RUNNER="${JAVA_HOME}/bin/java"
else
  if [ "$(command -v java)" ]; then
    RUNNER="java"
  else
    echo "JAVA_HOME is not set" >&2
    exit 1
  fi
fi

build_command() {
  "$RUNNER" -Xmx128m -cp "$LAUNCH_CLASSPATH" org.apache.spark.launcher.Main "$@"
  printf "%d\0" $?
}

# Turn off posix mode since it does not allow process substitution
set +o posix
CMD=()
while IFS= read -d '' -r ARG; do
  CMD+=("$ARG")
done < <(build_command "$@")
```

spark启动以`SparkSubmit`为主类的JVM进程

### jmx监控
在spark-shell中找到如下配置:
```shell
# SPARK-4161: scala does not assume use of the java classpath,
# so we need to add the "-Dscala.usejavacp=true" flag manually. We
# do this specifically for the Spark shell because the scala REPL
# has its own class loader, and any additional classpath specified
# through spark.driver.extraClassPath is not automatically propagated.
SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -Dscala.usejavacp=true"
```
修改为
```shell
SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -Dscala.usejavacp=true -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=10207 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
```

启动`jvisualvm`，打开本地或全程JMX的`org.apache.spark.deploy.SparkSubmit`进程,点击"线程"item，点击"main"线程，然后点击”线程Dump“，会dump线程，拖到最下面
```text
"main" #1 prio=5 os_prio=31 tid=0x00007f9202000800 nid=0x2503 runnable [0x0000700002997000]
   java.lang.Thread.State: RUNNABLE
        at java.io.FileInputStream.read0(Native Method)
        at java.io.FileInputStream.read(FileInputStream.java:207)
        at jline.internal.NonBlockingInputStream.read(Redefined)
        - locked <0x000000078368fb18> (a jline.internal.NonBlockingInputStream)
        at jline.internal.NonBlockingInputStream.read(Redefined)
        at jline.internal.NonBlockingInputStream.read(Redefined)
        at jline.internal.InputStreamReader.read(InputStreamReader.java:261)
        - locked <0x000000078368fb18> (a jline.internal.NonBlockingInputStream)
        at jline.internal.InputStreamReader.read(InputStreamReader.java:198)
        - locked <0x000000078368fb18> (a jline.internal.NonBlockingInputStream)
        at jline.console.ConsoleReader.readCharacter(ConsoleReader.java:2145)
        at jline.console.ConsoleReader.readLine(ConsoleReader.java:2349)
        at jline.console.ConsoleReader.readLine(ConsoleReader.java:2269)
        at scala.tools.nsc.interpreter.jline.InteractiveReader.readOneLine(JLineReader.scala:57)
        at scala.tools.nsc.interpreter.InteractiveReader$$anonfun$readLine$2.apply(InteractiveReader.scala:37)
        at scala.tools.nsc.interpreter.InteractiveReader$$anonfun$readLine$2.apply(InteractiveReader.scala:37)
        at scala.tools.nsc.interpreter.InteractiveReader$.restartSysCalls(InteractiveReader.scala:44)
        at scala.tools.nsc.interpreter.InteractiveReader$class.readLine(InteractiveReader.scala:37)
        at scala.tools.nsc.interpreter.jline.InteractiveReader.readLine(JLineReader.scala:28)
        at scala.tools.nsc.interpreter.ILoop.readOneLine(ILoop.scala:404)
        at scala.tools.nsc.interpreter.ILoop.loop(ILoop.scala:413)
        at scala.tools.nsc.interpreter.ILoop$$anonfun$process$1.apply$mcZ$sp(ILoop.scala:923)
        at scala.tools.nsc.interpreter.ILoop$$anonfun$process$1.apply(ILoop.scala:909)
        at scala.tools.nsc.interpreter.ILoop$$anonfun$process$1.apply(ILoop.scala:909)
        at scala.reflect.internal.util.ScalaClassLoader$.savingContextLoader(ScalaClassLoader.scala:97)
        at scala.tools.nsc.interpreter.ILoop.process(ILoop.scala:909)
        at org.apache.spark.repl.Main$.doMain(Main.scala:76)
        at org.apache.spark.repl.Main$.main(Main.scala:56)
        at org.apache.spark.repl.Main.main(Main.scala)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Redefined)
        at org.apache.spark.deploy.JavaMainApplication.start(Redefined)
        at org.apache.spark.deploy.SparkSubmit$.org$apache$spark$deploy$SparkSubmit$$runMain(Redefined)
        at org.apache.spark.deploy.SparkSubmit$.doRunMain$1(Redefined)
        at org.apache.spark.deploy.SparkSubmit$.submit(Redefined)
        at org.apache.spark.deploy.SparkSubmit$.main(Redefined)
        at org.apache.spark.deploy.SparkSubmit.main(Redefined)
```
可以看出函数调用顺序:
SparkSubmit.main ---> Main.main ---> ILoop.process







