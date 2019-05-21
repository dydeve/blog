---
title: spark-initializePipeline
my: spark-initializePipeline
date: 2019-05-21 14:00:08
 - spark
 - The Art of Spark Kernel Design
---
在[TransportClientFactory](../../21/spark-TransportClientFactory)的`createClient`,[TransportServer](../../21/spark-TransportServer)中的`init`方法，都会用到`TransportContext#initializePipeline`方法

```java
  //被TransportClientFactory#createClient调用
  public TransportChannelHandler initializePipeline(SocketChannel channel) {
    return initializePipeline(channel, rpcHandler);
  }

  /**
   * 被TransportServer#init 和 上面的 initializePipeline 调用
   * Initializes a client or server Netty Channel Pipeline which encodes/decodes messages and
   * has a {@link org.apache.spark.network.server.TransportChannelHandler} to handle request or
   * response messages.
   *
   * @param channel The channel to initialize.
   * @param channelRpcHandler The RPC handler to use for the channel.
   *
   * @return Returns the created TransportChannelHandler, which includes a TransportClient that can
   * be used to communicate on this channel. The TransportClient is directly associated with a
   * ChannelHandler to ensure all users of the same channel get the same TransportClient object.
   */
  public TransportChannelHandler initializePipeline(
      SocketChannel channel,
      RpcHandler channelRpcHandler) {
    try {
      TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);
      channel.pipeline()
        .addLast("encoder", ENCODER)
        .addLast(TransportFrameDecoder.HANDLER_NAME, NettyUtils.createFrameDecoder())
        .addLast("decoder", DECODER)
        .addLast("idleStateHandler", new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
        // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
        // would require more logic to guarantee if this were not part of the same event loop.
        .addLast("handler", channelHandler);
      return channelHandler;
    } catch (RuntimeException e) {
      logger.error("Error while initializing Netty pipeline", e);
      throw e;
    }
  }

  /**
   * Creates the server- and client-side handler which is used to handle both RequestMessages and
   * ResponseMessages. The channel is expected to have been successfully created, though certain
   * properties (such as the remoteAddress()) may not be available yet.
   */
  private TransportChannelHandler createChannelHandler(Channel channel, RpcHandler rpcHandler) {
    TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
    //实际创建client，在与RpcHandler无关，在TransportClientFactory中通过clientRef.set(clientHandler.getClient())设置
    TransportClient client = new TransportClient(channel, responseHandler);
    TransportRequestHandler requestHandler = new TransportRequestHandler(channel, client,
      rpcHandler, conf.maxChunksBeingTransferred());
    //TransportChannelHandler在服务端代理TransportRequestHandler处理RequestMessages，在客户端代理TransportResponseHandler处理ResponseMessages
    return new TransportChannelHandler(client, responseHandler, requestHandler,
      conf.connectionTimeoutMs(), closeIdleConnections);
  }
```

compoment | implements 
--- | --
TransportFrameDecoder | ChannelInboundHandler
MessageDecoder | ChannelInboundHandler
IdleStateHandler | ChannelInboundHandler
TransportChannelHandler | ChannelInboundHandler
MessageEncoder |  ChannelOutboundHandler

在netty中，`ChannelInboundHandler`按注册的先后顺序来、`ChannelOutboundHandler`按注册的先后逆序来


