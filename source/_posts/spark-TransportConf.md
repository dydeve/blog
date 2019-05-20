---
title: spark-TransportConf详解
my: spark-TransportConf
date: 2019-05-20 15:50:51
 - spark
 - big data
categories: 
 - spark
 - The Art of Spark Kernel Design
---
`TransportConf`源码的解释为：A central location that tracks all the settings we expose to users.追踪所有暴露给用户的配置。在SparkContext中，用于给RPC框架提供配置信息

`TransportConf`有两个属性：

1. ConfigProvider conf：配置提供者
2. String module：模块


```java
/**
 * Provides a mechanism for constructing a {@link TransportConf} using some sort of configuration.
 */
public abstract class ConfigProvider {
  /** Obtains the value of the given config, throws NoSuchElementException if it doesn't exist. */
  public abstract String get(String name);

  /** Returns all the config values in the provider. */
  public abstract Iterable<Map.Entry<String, String>> getAll();

  public String get(String name, String defaultValue) {
    try {
      return get(name);
    } catch (NoSuchElementException e) {
      return defaultValue;
    }
  }

  public int getInt(String name, int defaultValue) {
    return Integer.parseInt(get(name, Integer.toString(defaultValue)));
  }

  public long getLong(String name, long defaultValue) {
    return Long.parseLong(get(name, Long.toString(defaultValue)));
  }

  public double getDouble(String name, double defaultValue) {
    return Double.parseDouble(get(name, Double.toString(defaultValue)));
  }

  public boolean getBoolean(String name, boolean defaultValue) {
    return Boolean.parseBoolean(get(name, Boolean.toString(defaultValue)));
  }

}
```
ConfigProvider比较简单.`MapConfigProvider`是它的实现
```java
public class MapConfigProvider extends ConfigProvider {

  public static final MapConfigProvider EMPTY = new MapConfigProvider(Collections.emptyMap());

  private final Map<String, String> config;

  public MapConfigProvider(Map<String, String> config) {
    this.config = new HashMap<>(config);
  }

  @Override
  public String get(String name) {
    String value = config.get(name);
    if (value == null) {
      throw new NoSuchElementException(name);
    }
    return value;
  }

  @Override
  public String get(String name, String defaultValue) {
    String value = config.get(name);
    return value == null ? defaultValue : value;
  }

  @Override
  public Iterable<Map.Entry<String, String>> getAll() {
    return config.entrySet();
  }

}
```

```java
public class TransportConf {

  private final String SPARK_NETWORK_IO_MODE_KEY;
  
  ...

  private final ConfigProvider conf;

  private final String module;

  public TransportConf(String module, ConfigProvider conf) {
    this.module = module;
    this.conf = conf;
    SPARK_NETWORK_IO_MODE_KEY = getConfKey("io.mode");
    ...
  }

  public int getInt(String name, int defaultValue) {
    return conf.getInt(name, defaultValue);
  }

  public String get(String name, String defaultValue) {
    return conf.get(name, defaultValue);
  }

  private String getConfKey(String suffix) {
    return "spark." + module + "." + suffix;
  }

  public String getModuleName() {
    return module;
  }

  /** IO mode: nio or epoll */
  public String ioMode() {
    return conf.get(SPARK_NETWORK_IO_MODE_KEY, "NIO").toUpperCase(Locale.ROOT);
  }
```
可以看出，"spark." + module + ".suffix"就得到了key，用conf.getXxx(key)得到具体value(个人感觉这是鸡肋，没什么太大作用)。

spark通常使用`SparkTransportConf`创建`TransportConf`
```java
/**
 * Provides a utility for transforming from a SparkConf inside a Spark JVM (e.g., Executor,
 * Driver, or a standalone shuffle service) into a TransportConf with details on our environment
 * like the number of cores that are allocated to this JVM.
 */
object SparkTransportConf {
  /**
   * Specifies an upper bound on the number of Netty threads that Spark requires by default.
   * In practice, only 2-4 cores should be required to transfer roughly 10 Gb/s, and each core
   * that we use will have an initial overhead of roughly 32 MB of off-heap memory, which comes
   * at a premium.
   *
   * Thus, this value should still retain maximum throughput and reduce wasted off-heap memory
   * allocation. It can be overridden by setting the number of serverThreads and clientThreads
   * manually in Spark's configuration.
   */
  private val MAX_DEFAULT_NETTY_THREADS = 8

  /**
   * Utility for creating a [[TransportConf]] from a [[SparkConf]].
   * @param _conf the [[SparkConf]]
   * @param module the module name
   * @param numUsableCores if nonzero, this will restrict the server and client threads to only
   *                       use the given number of cores, rather than all of the machine's cores.
   *                       This restriction will only occur if these properties are not already set.
   */
  def fromSparkConf(_conf: SparkConf, module: String, numUsableCores: Int = 0): TransportConf = {
    val conf = _conf.clone

    // Specify thread configuration based on our JVM's allocation of cores (rather than necessarily
    // assuming we have all the machine's cores).
    // NB: Only set if serverThreads/clientThreads not already set.
    val numThreads = defaultNumThreads(numUsableCores)
    conf.setIfMissing(s"spark.$module.io.serverThreads", numThreads.toString)
    conf.setIfMissing(s"spark.$module.io.clientThreads", numThreads.toString)

    new TransportConf(module, new ConfigProvider {
      override def get(name: String): String = conf.get(name)
      override def get(name: String, defaultValue: String): String = conf.get(name, defaultValue)
      override def getAll(): java.lang.Iterable[java.util.Map.Entry[String, String]] = {
        conf.getAll.toMap.asJava.entrySet()
      }
    })
  }

  /**
   * Returns the default number of threads for both the Netty client and server thread pools.
   * If numUsableCores is 0, we will use Runtime get an approximate number of available cores.
   */
  private def defaultNumThreads(numUsableCores: Int): Int = {
    val availableCores =
      if (numUsableCores > 0) numUsableCores else Runtime.getRuntime.availableProcessors()
    math.min(availableCores, MAX_DEFAULT_NETTY_THREADS)
  }
}
```
重点有两个：
1. 如果 numUsableCores <= 0，那么线程数是系统可用处理器的数量，但是系统的cores不可能全部用于网络传输使用，所以这里还将分配给网络传输的内核数量最多限制在8个

最终确认线程数的，以SparkConf的配置优先:
```scala
conf.setIfMissing(s"spark.$module.io.serverThreads", numThreads.toString)
conf.setIfMissing(s"spark.$module.io.clientThreads", numThreads.toString)
```
2. 构造一个ConfigProvider的匿名内部类，get的实现实际是代理了`SparkConf`的get方法

