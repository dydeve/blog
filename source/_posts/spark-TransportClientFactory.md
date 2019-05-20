---
title: spark-TransportClientFactory详解
my: spark-TransportClientFactory
date: 2019-05-20 17:27:57
 - spark
 - big data
categories: 
 - spark
 - The Art of Spark Kernel Design
---

### TransportClientFactory

 `TransportClientFactory`是创建`TransportClient`的工厂类

 构造方法
 ```java
  public TransportClientFactory(
      TransportContext context,
      List<TransportClientBootstrap> clientBootstraps) {
    this.context = Preconditions.checkNotNull(context);
    this.conf = context.getConf();
    this.clientBootstraps = Lists.newArrayList(Preconditions.checkNotNull(clientBootstraps));
    this.connectionPool = new ConcurrentHashMap<>();
    this.numConnectionsPerPeer = conf.numConnectionsPerPeer();
    this.rand = new Random();

    IOMode ioMode = IOMode.valueOf(conf.ioMode());
    this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
    this.workerGroup = NettyUtils.createEventLoop(
        ioMode,
        conf.clientThreads(),
        conf.getModuleName() + "-client");
    this.pooledAllocator = NettyUtils.createPooledByteBufAllocator(
      conf.preferDirectBufs(), false /* allowCache */, conf.clientThreads());
    this.metrics = new NettyMemoryMetrics(
      this.pooledAllocator, conf.getModuleName() + "-client", conf);
  }
 ```
TransportClientBootstrap：A bootstrap which is executed on a TransportClient before it is returned to the user.

connectionPool： ConcurrentHashMap<SocketAddress, ClientPool>,ClientPool[TransportClient[] clients, Object[] locks]

rand: `createClient(String remoteHost, int remotePort)`中，用于从`clientPool.clients`中获取`TransportClient cachedClient`，负载均衡

numConnectionsPerPeer：Number of concurrent connections between two nodes for fetching data.用于构造`connectionPool.putIfAbsent(unresolvedAddress, new ClientPool(numConnectionsPerPeer))`。

ioMode：从conf中获取，有`NIO, EPOLL`两种。NIO总是可用，EPOLL只可用于Linux

workerGroup：根据Netty的规范，客户端只有worker组.有`NioEventLoopGroup`、`EpollEventLoopGroup`两种情况

pooledAllocator：汇集ByteBuf但对本地线程缓存禁用的分配器

### TransportClientBootstrap 客户端引导程序
在`TransportClient`返回用户前，做一些操作。可以做一些昂贵操作，因为`TransportClient`会尽可能重用
```java
/**
 * A bootstrap which is executed on a TransportClient before it is returned to the user.
 * This enables an initial exchange of information (e.g., SASL authentication tokens) on a once-per-
 * connection basis.
 *
 * Since connections (and TransportClients) are reused as much as possible, it is generally
 * reasonable to perform an expensive bootstrapping operation, as they often share a lifespan with
 * the JVM itself.
 */
public interface TransportClientBootstrap {
  /** Performs the bootstrapping operation, throwing an exception on failure. */
  void doBootstrap(TransportClient client, Channel channel) throws RuntimeException;
}
```

`TransportClientFactory#createClient`
```java
/**
 * Factory for creating {@link TransportClient}s by using createClient.
 *
 * The factory maintains a connection pool to other hosts and should return the same
 * TransportClient for the same remote host. It also shares a single worker thread pool for
 * all TransportClients.
 *
 * TransportClients will be reused whenever possible. Prior to completing the creation of a new
 * TransportClient, all given {@link TransportClientBootstrap}s will be run.
 */
public class TransportClientFactory implements Closeable {
  /** Create a completely new {@link TransportClient} to the remote address. */
  private TransportClient createClient(InetSocketAddress address)
      throws IOException, InterruptedException {
    logger.debug("Creating new connection to {}", address);
    //构建根引导程序Bootstrap，并对其配置
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(workerGroup)
      .channel(socketChannelClass)
      // Disable Nagle's Algorithm since we don't want packets to wait
      .option(ChannelOption.TCP_NODELAY, true)
      .option(ChannelOption.SO_KEEPALIVE, true)
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectionTimeoutMs())
      .option(ChannelOption.ALLOCATOR, pooledAllocator);

    if (conf.receiveBuf() > 0) {
      bootstrap.option(ChannelOption.SO_RCVBUF, conf.receiveBuf());
    }

    if (conf.sendBuf() > 0) {
      bootstrap.option(ChannelOption.SO_SNDBUF, conf.sendBuf());
    }

    final AtomicReference<TransportClient> clientRef = new AtomicReference<>();
    final AtomicReference<Channel> channelRef = new AtomicReference<>();

    //为根引导程序设置管道初始化回调函数
    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) {
        TransportChannelHandler clientHandler = context.initializePipeline(ch);
        clientRef.set(clientHandler.getClient());
        channelRef.set(ch);
      }
    });

    // Connect to the remote server
    long preConnect = System.nanoTime();
    ChannelFuture cf = bootstrap.connect(address);
    if (!cf.await(conf.connectionTimeoutMs())) {
      throw new IOException(
        String.format("Connecting to %s timed out (%s ms)", address, conf.connectionTimeoutMs()));
    } else if (cf.cause() != null) {
      throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
    }

    TransportClient client = clientRef.get();
    Channel channel = channelRef.get();
    assert client != null : "Channel future completed successfully with null client";

    // Execute any client bootstraps synchronously before marking the Client as successful.
    long preBootstrap = System.nanoTime();
    logger.debug("Connection to {} successful, running bootstraps...", address);
    try {
      for (TransportClientBootstrap clientBootstrap : clientBootstraps) {
        clientBootstrap.doBootstrap(client, channel);//客户单引导程序
      }
    } catch (Exception e) { // catch non-RuntimeExceptions too as bootstrap may be written in Scala
      long bootstrapTimeMs = (System.nanoTime() - preBootstrap) / 1000000;
      logger.error("Exception while bootstrapping client after " + bootstrapTimeMs + " ms", e);
      client.close();
      throw Throwables.propagate(e);
    }
    long postBootstrap = System.nanoTime();

    logger.info("Successfully created connection to {} after {} ms ({} ms spent in bootstraps)",
      address, (postBootstrap - preConnect) / 1000000, (postBootstrap - preBootstrap) / 1000000);

    return client;
  }
}
```
return client之前，在for循环里`clientBootstrap.doBootstrap(client, channel)`。这是个private方法，在其他两处被调用


### 创建rpc客户端 TransportClient

```java
  /**
   * Create a {@link TransportClient} connecting to the given remote host / port.
   *
   * We maintains an array of clients (size determined by spark.shuffle.io.numConnectionsPerPeer)
   * and randomly picks one to use. If no client was previously created in the randomly selected
   * spot, this function creates a new client and places it there.
   *
   * Prior to the creation of a new TransportClient, we will execute all
   * {@link TransportClientBootstrap}s that are registered with this factory.
   *
   * This blocks until a connection is successfully established and fully bootstrapped.
   *
   * Concurrency: This method is safe to call from multiple threads.
   */
  public TransportClient createClient(String remoteHost, int remotePort)
      throws IOException, InterruptedException {
    // Get connection from the connection pool first.
    // If it is not found or not active, create a new one.
    // Use unresolved address here to avoid DNS resolution each time we creates a client.
    final InetSocketAddress unresolvedAddress =
      InetSocketAddress.createUnresolved(remoteHost, remotePort);//避免dns解析

    // Create the ClientPool if we don't have it yet.只创建ClientPool，不创建TransportClient
    ClientPool clientPool = connectionPool.get(unresolvedAddress);
    if (clientPool == null) {
      connectionPool.putIfAbsent(unresolvedAddress, new ClientPool(numConnectionsPerPeer));
      clientPool = connectionPool.get(unresolvedAddress);//并发安全
    }

    int clientIndex = rand.nextInt(numConnectionsPerPeer);
    TransportClient cachedClient = clientPool.clients[clientIndex];//随机选、负载均衡

    if (cachedClient != null && cachedClient.isActive()) {//ClientPool里有TransportClient
      // Make sure that the channel will not timeout by updating the last use time of the
      // handler. Then check that the client is still alive, in case it timed out before
      // this code was able to update things.
      TransportChannelHandler handler = cachedClient.getChannel().pipeline()
        .get(TransportChannelHandler.class);
      synchronized (handler) {
        handler.getResponseHandler().updateTimeOfLastRequest();
      }

      if (cachedClient.isActive()) {
        logger.trace("Returning cached connection to {}: {}",
          cachedClient.getSocketAddress(), cachedClient);
        return cachedClient;
      }
    }

    // If we reach here, we don't have an existing connection open. Let's create a new one.
    // Multiple threads might race here to create new connections. Keep only one of them active.
    final long preResolveHost = System.nanoTime();
    final InetSocketAddress resolvedAddress = new InetSocketAddress(remoteHost, remotePort);//会有dns解析
    final long hostResolveTimeMs = (System.nanoTime() - preResolveHost) / 1000000;
    if (hostResolveTimeMs > 2000) {
      logger.warn("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
    } else {
      logger.trace("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
    }

    synchronized (clientPool.locks[clientIndex]) {//锁分段
      cachedClient = clientPool.clients[clientIndex];

      if (cachedClient != null) {
        if (cachedClient.isActive()) {//双重检验
          logger.trace("Returning cached connection to {}: {}", resolvedAddress, cachedClient);
          return cachedClient;
        } else {
          logger.info("Found inactive connection to {}, creating a new one.", resolvedAddress);
        }
      }
      clientPool.clients[clientIndex] = createClient(resolvedAddress);
      return clientPool.clients[clientIndex];//调用上面的私有方法
    }
  }

  /**
   * 直接创建、不从缓存取
   * Create a completely new {@link TransportClient} to the given remote host / port.
   * This connection is not pooled.
   *
   * As with {@link #createClient(String, int)}, this method is blocking.
   */
  public TransportClient createUnmanagedClient(String remoteHost, int remotePort)
      throws IOException, InterruptedException {
    final InetSocketAddress address = new InetSocketAddress(remoteHost, remotePort);
    return createClient(address);
  }
```


```java
public class InetSocketAddress
    extends SocketAddress
{
    // Private implementation class pointed to by all public methods.
    private static class InetSocketAddressHolder {
        // The hostname of the Socket Address
        private String hostname;
        // The IP address of the Socket Address
        private InetAddress addr;
        // The port number of the Socket Address
        private int port;

        ...

        private boolean isUnresolved() {
            return addr == null;
        }

        @Override
        public final boolean equals(Object obj) {
            if (obj == null || !(obj instanceof InetSocketAddressHolder))
                return false;
            InetSocketAddressHolder that = (InetSocketAddressHolder)obj;
            boolean sameIP;
            if (addr != null)
                sameIP = addr.equals(that.addr);
            else if (hostname != null)
                sameIP = (that.addr == null) &&
                    hostname.equalsIgnoreCase(that.hostname);
            else
                sameIP = (that.addr == null) && (that.hostname == null);
            return sameIP && (port == that.port);
        }

        @Override
        public final int hashCode() {
            if (addr != null)
                return addr.hashCode() + port;
            if (hostname != null)
                return hostname.toLowerCase().hashCode() + port;
            return port;
        }
    }

    private final transient InetSocketAddressHolder holder;
    public InetSocketAddress(String hostname, int port) {
        checkHost(hostname);
        InetAddress addr = null;
        String host = null;
        try {
            addr = InetAddress.getByName(hostname);//DNS解析
        } catch(UnknownHostException e) {
            host = hostname;
        }
        holder = new InetSocketAddressHolder(host, addr, checkPort(port));
    }

    // private constructor for creating unresolved instances
    private InetSocketAddress(int port, String hostname) {
        holder = new InetSocketAddressHolder(hostname, null, port);//无dns解析
    }

    public static InetSocketAddress createUnresolved(String host, int port) {
        return new InetSocketAddress(checkPort(port), checkHost(host));
    }

    /**
     * Returns a hashcode for this socket address.
     *
     * @return  a hash code value for this socket address.
     */
    @Override
    public final int hashCode() {
        return holder.hashCode();
    }
}
```


