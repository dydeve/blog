---
title: spark rpc
my: spark-rpc
date: 2019-05-20 10:15:38
tags: 
 - spark
 - big data
categories: 
 - spark
 - The Art of Spark Kernel Design
---
### spark内置RPC框架
Spark组件间的消息互通、用户文件与Jar包的上传、节点间的Shuffle过程、Block数据的复制与备份等都用到Spark RPC

![](./spark-infrastructure/spark-rpc.png)

#### Spark RPC各组件简介

TransportContext：Contains the context to create a TransportServer, TransportClientFactory, and to setup Netty Channel pipelines with a TransportChannelHandler.

TransportConf：A central location that tracks all the settings we expose to users。用于创建TransportClientFactory、TransportServer

RpcHandler：Handler for `sendRPC()` messages sent by `link org.apache.spark.network.client.TransportClient`s.只用于创建TransportServer

MessageEncoder：服务端用于给`server-to-client responses`编码，无状态，多线程安全

MessageDecoder：客户端将`server-to-client responses`解码，无状态，多线程安全

RpcResponseCallback：Callback for the result of a single RPC. This will be invoked once with either success or failure

TransportClientBootstrap：A bootstrap which is executed on a TransportClient before it is returned to the user.

TransportRequestHandler：A handler that processes requests from clients and writes chunk data back.

TransportResponseHandler：Handler that processes server responses

TransportChannelHandler：The single Transport-level Channel handler which is used for delegating requests to the TransportRequestHandler and responses to the TransportResponseHandler.

TransportServerBootstrap：A bootstrap which is executed on a TransportServer's client channel once a client connects to the server.

TransportClientFactory包含针对每个Socket地址的连接池
```scala
  private final ConcurrentHashMap<SocketAddress, ClientPool> connectionPool;

  /** A simple data structure to track the pool of clients between two peer nodes. */
  private static class ClientPool {
    TransportClient[] clients;
    Object[] locks;

    //对不同的TransportClient采用不同的锁，类似于锁分段，降低并发下的锁争用
    ClientPool(int size) {
      clients = new TransportClient[size];
      locks = new Object[size];
      for (int i = 0; i < size; i++) {
        locks[i] = new Object();
      }
    }
  }
```
#### Spark RPC各组件详解

[spark-TransportConf](../../21/spark-TransportConf)

[spark-TransportClientFactory](../../21/spark-TransportClientFactory)

[spark-TransportServer](../../21/spark-TransportServer)

[spark-管道初始化](../../21/spark-initializePipeline)

先告一段落

RPC传输管道处理器TransportChannelHandler详解
服务端RpcHandler详解
服务端引导程序TransportServerBootstrap
客户端TransportClient详解
