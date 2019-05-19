---
title: spark设计理念和基本架构
my: spark-design-concept-basic-architecture
date: 2019-05-19 14:42:57
tags: 
 - spark
 - big data
categories: 
 - spark
 - The Art of Spark Kernel Design
---

### spark核心功能

1. 基础设施

包括SparkConf、内置RPC框架、事件总线(ListenerBus)、度量系统
    
    1. SparkConf管理Spark配置信息
    2. RPC框架由netty实现，有同步、异步多种实现,跨机器节点不同组件的通信设施，用于各组件间的通信
    3. ListenerBus是sparkContext内部各组件使用事件-监听器模式异步调用
    4. 度量系统(MetricSystem)，由多种度量源、度量输出(Source、Sink)构成，完成各组件运行期状态的监控

2. SparkContext

    隐藏了网络通信、分布式部署、消息通信、存储体系、计算引擎、度量系统、文件服务、webui

3. SparkEnv

    task运行必须的组件。封装了RPC环境(RpcEnv)、序列化管理器、广播管理器(BroadcastManager)、Map输出跟踪器(MapOutputTracker)、存储体系、度量系统(MetricsSystem)、输出提交协调器(OutputCommitCoordinator)

4. 存储体系

    优先内存、其次磁盘.Spark的内存存储空间、执行存储空间之间可以是软边界，资源紧张的一方可以借用另一方的空间。还提供`Tungsten`的实现，直接操作os的内存，空间的分配、释放更加迅速。而且省去在堆内分配java对象，更有效利用系统内存资源

5. 调度系统

    内置于SparkContext的DAGScheduler、TaskScheduler构成

    DAGScheduler负责创建job，将DAG中的RDD划分到不同Stage、给Stage创建对应的task，批量提交task

    TaskScheduler按FIFO、FAIR等调度算法对批量Task进行调度，为task分配资源，发送task到Executor

6. 计算引擎

    由内存管理器(MemoryManager)、Tungsten、任务内存管理器(TaskMemoryManager)、Task、外部排序器(ExternalSorter)、Shuffle管理器(ShuffleManager)组成

    MemoryManager为存储内存、计算引擎中的执行内存呢提供支持、管理

    Tungsten 用于存储、计算执行

    TaskMemoryManager为分配给单个task的内存资源进行更细粒度的管理控制

    ExternalSorter在map、reduce端对ShuffleMapTask计算得到的中间结果排序、聚合

    ShuffleManager将各个分区ShuffleMapTask产生的中间结果持久化到磁盘，在reduce端按分区远程拉取中间结果


### spark扩展功能
spark sql、spark streaming、graphx、mllib等

### 基本架构
![组成](https://spark.apache.org/docs/2.4.3/img/cluster-overview.png)

1. ClusterManager 负责集群资源的管理分配。分配的资源属于一级分配，将worker的内存、cpu等资源分配给application。不负责对executor的资源分配

2. worker 将内存、cpu通过注册机制告知ClusterManager；创建executor，分配资源、任务给executor；同步资源信息、executor状态给ClusterManager。standalone模式下，master将worker的资源分配给application后，将命令worker启动`CoarseGrainedExecutorBackend`进程(此进程创建executor实例),

3. executor，执行任务，与worker、driver信息同步

4. driver，application的驱动程序。application通过driver与ClusterManager、executor通信。可运行在application中，或由app提交给ClusterManager，由ClusterManager安排worker运行

5. application：转换RDD，构建DAG，通过driver注册到ClusterManager。ClusterManager根据app的资源需求，通过一级分配将executor、内存、cpu分配给app；driver通过二级分配将executor等资源分配给task。app通过driver告诉executor执行任务

### 具体流程

通过`SparkContext`提交的用户应用程序，首先会通过`RpcEnv`向`ClusterManager`注册应用(Application)，并告知需要的资源数量。ClusterManager给application分配executor资源，
并在worker上启动`CoarseGrainedExecutorBackend`进程,进程启动过程中通过RpcEnv向driver注册executor的资源信息。

`TaskScheduler`保存executor的资源信息。SparkContext构建RDD的lineage和DAG。DAG提交给`DAGScheduler`，DAGScheduler给提交的DAG创建job，将dag划分为Stage。DAGScheduler根据RDD内partition的数量创建task并批量提交给TaskScheduler。TaskScheduler对task按FIFO、FAIR等调度算法调度，将task发送给executor

sparkContext会在RDD转换前用`BlockManager`和`BroadcastManager`将任务的hadoop配置进行广播

---
