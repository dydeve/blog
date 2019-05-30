---
title: 向 clickhouse-jdbc 学习 jdbc
my: clickhouse-jdbc
date: 2019-05-27 23:22:16
tags: 
 - clickhouse
 - jdbc
 - httpclient
categories: util
---

[clickhouse-jdbc](https://github.com/yandex/clickhouse-jdbc)是[clickhouse](https://clickhouse.yandex/)的jdbc驱动，本文从clickhouse-jdbc探索`jdbc`的一般实现

---

#### ClickHouseDriver
```java
/**
 *
 * URL Format
 *
 * primitive for now
 *
 * jdbc:clickhouse://host:port
 *
 * for example, jdbc:clickhouse://localhost:8123
 *
 */
public class ClickHouseDriver implements Driver {

    private static final Logger logger = LoggerFactory.getLogger(ClickHouseDriver.class);

    private static final ConcurrentMap<ClickHouseConnectionImpl, Boolean> connections = new MapMaker().weakKeys().makeMap();

    static {
        ClickHouseDriver driver = new ClickHouseDriver();
        try {
            DriverManager.registerDriver(driver);//注册驱动
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        logger.info("Driver registered");
    }

    @Override
    public ClickHouseConnection connect(String url, Properties info) throws SQLException {
        return connect(url, new ClickHouseProperties(info));
    }

    public ClickHouseConnection connect(String url, ClickHouseProperties properties) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        logger.debug("Creating connection");
        ClickHouseConnectionImpl connection = new ClickHouseConnectionImpl(url, properties);
        registerConnection(connection);
        return LogProxy.wrap(ClickHouseConnection.class, connection);
    }

    private void registerConnection(ClickHouseConnectionImpl connection) {
        connections.put(connection, Boolean.TRUE);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX);
    }

    //...

    /**
     * Schedules connections cleaning at a rate. Turned off by default.
     * See https://hc.apache.org/httpcomponents-client-4.5.x/tutorial/html/connmgmt.html#d5e418
     * 定时清理连接，本质上清理httpclient
     * @param rate
     * @param timeUnit
     */
    public void scheduleConnectionsCleaning(int rate, TimeUnit timeUnit){
        ScheduledConnectionCleaner.INSTANCE.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    for (ClickHouseConnectionImpl connection : connections.keySet()) {
                        connection.cleanConnections();
                    }
                } catch (Exception e){
                    logger.error("error evicting connections: " + e);
                }
            }
        }, 0, rate, timeUnit);
    }

    static class ScheduledConnectionCleaner {
        static final ScheduledExecutorService INSTANCE = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory());

        static class DaemonThreadFactory implements ThreadFactory {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setDaemon(true);
                return thread;
            }
        }
    }
}
```

主要是构建`ClickHouseConnectionImpl`，实现`Driver`#connect(String url, java.util.Properties info) throws SQLException 方法

#### ClickHouseConnectionImpl

```java
public interface ClickHouseConnection extends Connection {

    @Deprecated
    ClickHouseStatement createClickHouseStatement() throws SQLException;

    TimeZone getTimeZone();

    // 将返回值类型 Statement 改为 ClickHouseStatement，更灵活
    @Override
    ClickHouseStatement createStatement() throws SQLException;

    @Override
    ClickHouseStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException;

    String getServerVersion() throws SQLException;
}
```

```java
public class ClickHouseConnectionImpl implements ClickHouseConnection {
	
	private static final int DEFAULT_RESULTSET_TYPE = ResultSet.TYPE_FORWARD_ONLY;
	
    private static final Logger log = LoggerFactory.getLogger(ClickHouseConnectionImpl.class);

    private final CloseableHttpClient httpclient;

    private final ClickHouseProperties properties;

    private String url;

    private boolean closed = false;

    private TimeZone timezone;
    private volatile String serverVersion;

    public ClickHouseConnectionImpl(String url) {
        this(url, new ClickHouseProperties());
    }

    // 可以看到，ClickHouseConnectionImpl 本质上是 httpclient
    public ClickHouseConnectionImpl(String url, ClickHouseProperties properties) {
        this.url = url;
        try {
            this.properties = ClickhouseJdbcUrlParser.parse(url, properties.asProperties());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        ClickHouseHttpClientBuilder clientBuilder = new ClickHouseHttpClientBuilder(this.properties);
        log.debug("new connection");
        try {
            httpclient = clientBuilder.buildClient();
        }catch (Exception e) {
            throw  new IllegalStateException("cannot initialize http client", e);
        }
        initTimeZone(this.properties);
    }

    public ClickHouseStatement createStatement(int resultSetType) throws SQLException {
        return LogProxy.wrap(ClickHouseStatement.class, new ClickHouseStatementImpl(httpclient, this, properties, resultSetType));
    }

    void cleanConnections() {
        httpclient.getConnectionManager().closeExpiredConnections();
        httpclient.getConnectionManager().closeIdleConnections(2 * properties.getSocketTimeout(), TimeUnit.MILLISECONDS);
    }

    // 主要作用是创建 ClickHouseStatement
}
```

#### ClickHouseStatementImpl
```java
public class ClickHouseStatementImpl implements ClickHouseStatement {

    @Override
    public ResultSet executeQuery(String sql,
                                  Map<ClickHouseQueryParam, String> additionalDBParams,
                                  List<ClickHouseExternalData> externalData,
                                  Map<String, String> additionalRequestParams) throws SQLException {

        // forcibly disable extremes for ResultSet queries
        if (additionalDBParams == null || additionalDBParams.isEmpty()) {
            additionalDBParams = new EnumMap<ClickHouseQueryParam, String>(ClickHouseQueryParam.class);
        } else {
            additionalDBParams = new EnumMap<ClickHouseQueryParam, String>(additionalDBParams);
        }
        additionalDBParams.put(ClickHouseQueryParam.EXTREMES, "0");

        InputStream is = getInputStream(sql, additionalDBParams, externalData, additionalRequestParams);

        try {
            if (isSelect(sql)) {
                currentUpdateCount = -1;
                currentResult = createResultSet(properties.isCompress()
                    ? new ClickHouseLZ4Stream(is) : is, properties.getBufferSize(),
                    extractDBName(sql),
                    extractTableName(sql),
                    extractWithTotals(sql),
                    this,
                    getConnection().getTimeZone(),
                    properties
                );
                currentResult.setMaxRows(maxRows);
                return currentResult;
            } else {
                currentUpdateCount = 0;
                StreamUtils.close(is);
                return null;
            }
        } catch (Exception e) {
            StreamUtils.close(is);
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    private ClickHouseResultSet createResultSet(InputStream is, int bufferSize, String db, String table, boolean usesWithTotals,
    		ClickHouseStatement statement, TimeZone timezone, ClickHouseProperties properties) throws IOException {
    	if(isResultSetScrollable) {
    		return new ClickHouseScrollableResultSet(is, bufferSize, db, table, usesWithTotals, statement, timezone, properties);
    	} else {
    		return new ClickHouseResultSet(is, bufferSize, db, table, usesWithTotals, statement, timezone, properties);
    	}
    }

}
```
executeQuery方法将sql语句组成参数，通过httpclient传给server，返回stream(会用到`FastByteArrayOutputStream`[去除synchronized的ByteArrayOutputStream])。然后把stream封装到`ClickHouseResultSet`

#### BalancedClickhouseDataSource
当调用getConnection方法，会返回连接到随机主机的connection.也可以周期检查连接是否活跃。
```java
/**
 * <p> Database for clickhouse jdbc connections.
 * <p> It has list of database urls.
 * For every {@link #getConnection() getConnection} invocation, it returns connection to random host from the list.
 * Furthermore, this class has method {@link #scheduleActualization(int, TimeUnit) scheduleActualization}
 * which test hosts for availability. By default, this option is turned off.
 */
public class BalancedClickhouseDataSource implements DataSource {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(BalancedClickhouseDataSource.class);
    private static final Pattern URL_TEMPLATE = Pattern.compile(JDBC_CLICKHOUSE_PREFIX + "" +
            "//([a-zA-Z0-9_:,.-]+)" +
            "(/[a-zA-Z0-9_]+" +
            "([?][a-zA-Z0-9_]+[=][a-zA-Z0-9_]+([&][a-zA-Z0-9_]+[=][a-zA-Z0-9_]+)*)?" +
            ")?");

    private PrintWriter printWriter;
    private int loginTimeoutSeconds = 0;

    private final ThreadLocal<Random> randomThreadLocal = new ThreadLocal<Random>();
    private final List<String> allUrls;
    private volatile List<String> enabledUrls;

    private final ClickHouseProperties properties;
    private final ClickHouseDriver driver = new ClickHouseDriver();

    private boolean ping(final String url) {
        try {
            driver.connect(url, properties).createStatement().execute("SELECT 1");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Checks if clickhouse on url is alive, if it isn't, disable url, else enable.
     *
     * @return number of avaliable clickhouse urls
     */
    public synchronized int actualize() {
        List<String> enabledUrls = new ArrayList<String>(allUrls.size());

        for (String url : allUrls) {
            log.debug("Pinging disabled url: {}", url);
            if (ping(url)) {
                log.debug("Url is alive now: {}", url);
                enabledUrls.add(url);
            } else {
                log.debug("Url is dead now: {}", url);
            }
        }

        this.enabledUrls = Collections.unmodifiableList(enabledUrls);
        return enabledUrls.size();
    }

    private String getAnyUrl() throws SQLException {
        List<String> localEnabledUrls = enabledUrls;
        if (localEnabledUrls.isEmpty()) {
            throw new SQLException("Unable to get connection: there are no enabled urls");
        }
        Random random = this.randomThreadLocal.get();
        if (random == null) {
            this.randomThreadLocal.set(new Random(System.currentTimeMillis()));
            random = this.randomThreadLocal.get();
        }

        int index = random.nextInt(localEnabledUrls.size());//随机选择链接
        return localEnabledUrls.get(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ClickHouseConnection getConnection() throws SQLException {
        return driver.connect(getAnyUrl(), properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ClickHouseConnection getConnection(String username, String password) throws SQLException {
        return driver.connect(getAnyUrl(), properties.withCredentials(username, password));
    }

    /**
     * set time period for checking availability connections
     * 周期检查可用性
     * @param delay    value for time unit
     * @param timeUnit time unit for checking
     * @return this datasource with changed settings
     */
    public BalancedClickhouseDataSource scheduleActualization(int delay, TimeUnit timeUnit) {
        ClickHouseDriver.ScheduledConnectionCleaner.INSTANCE.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    actualize();
                } catch (Exception e) {
                    log.error("Unable to actualize urls", e);
                }
            }
        }, 0, delay, timeUnit);

        return this;
    }

}
```

#### LogProxy
LogProxy在很多地方用到，主要功能是，当log可trace时，打印sql的上下文
```java
public class LogProxy<T> implements InvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(LogProxy.class);

    private final T object;
    private final Class<T> clazz;

    public static <T> T wrap(Class<T> interfaceClass, T object) {
        if (log.isTraceEnabled()) {//仅当日志级别为trace，返回代理对象
            LogProxy<T> proxy = new LogProxy<T>(interfaceClass, object);
            return proxy.getProxy();
        }
        return object;
    }

    private LogProxy(Class<T> interfaceClass, T object) {
        if (!interfaceClass.isInterface()) {
            throw new IllegalStateException("Class " + interfaceClass.getName() + " is not an interface");
        }
        clazz = interfaceClass;
        this.object = object;
    }

    @SuppressWarnings("unchecked")
    public T getProxy() {
        //xnoinspection x
        // unchecked
        return (T) Proxy.newProxyInstance(clazz.getClassLoader(), new Class<?>[]{clazz}, this);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String msg =
            "Call class: " + object.getClass().getName() +
            "\nMethod: " + method.getName() +
            "\nObject: " + object +
            "\nArgs: " + Arrays.toString(args) +
            "\nInvoke result: ";
        try {
            final Object invokeResult = method.invoke(object, args);
            msg +=  invokeResult;
            return invokeResult;
        } catch (InvocationTargetException e) {
            msg += e.getMessage();
            throw e.getTargetException();
        } finally {
            msg = "==== ClickHouse JDBC trace begin ====\n" + msg + "\n==== ClickHouse JDBC trace end ====";
            log.trace(msg);
        }
    }
}
```

