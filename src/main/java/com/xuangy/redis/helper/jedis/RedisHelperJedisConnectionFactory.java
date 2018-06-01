/*
 * Copyright 2011-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.xuangy.redis.helper.jedis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.PassThroughExceptionTranslationStrategy;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.jedis.JedisConverters;
import org.springframework.data.redis.connection.jedis.JedisSentinelConnection;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;
import redis.clients.jedis.*;
import redis.clients.util.Pool;

import java.lang.reflect.Method;
import java.util.*;

/**
 * Connection factory creating <a href="http://github.com/xetorthio/jedis">Jedis</a> based connections.
 * 
 * @author Costin Leau
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class RedisHelperJedisConnectionFactory implements InitializingBean, DisposableBean, RedisConnectionFactory {

	private final static Log log = LogFactory.getLog(RedisHelperJedisConnectionFactory.class);
	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new PassThroughExceptionTranslationStrategy(
			JedisConverters.exceptionConverter());

	private static final Method SET_TIMEOUT_METHOD;
	private static final Method GET_TIMEOUT_METHOD;

	static {

		// We need to configure Jedis socket timeout via reflection since the method-name was changed between releases.
		Method setTimeoutMethodCandidate = ReflectionUtils.findMethod(JedisShardInfo.class, "setTimeout", int.class);
		if (setTimeoutMethodCandidate == null) {
			// Jedis V 2.7.x changed the setTimeout method to setSoTimeout
			setTimeoutMethodCandidate = ReflectionUtils.findMethod(JedisShardInfo.class, "setSoTimeout", int.class);
		}
		SET_TIMEOUT_METHOD = setTimeoutMethodCandidate;

		Method getTimeoutMethodCandidate = ReflectionUtils.findMethod(JedisShardInfo.class, "getTimeout");
		if (getTimeoutMethodCandidate == null) {
			getTimeoutMethodCandidate = ReflectionUtils.findMethod(JedisShardInfo.class, "getSoTimeout");
		}

		GET_TIMEOUT_METHOD = getTimeoutMethodCandidate;
	}

	private JedisShardInfo shardInfo;
	private String hostName = "localhost";
	private int port = Protocol.DEFAULT_PORT;
	private int timeout = Protocol.DEFAULT_TIMEOUT;
	private String password;
	private boolean usePool = true;
	private Pool<Jedis> pool;
	private JedisPoolConfig poolConfig = new JedisPoolConfig();
	private int dbIndex = 0;
	private boolean convertPipelineAndTxResults = true;
	private RedisSentinelConfiguration sentinelConfig;
	private RedisClusterConfiguration clusterConfig;
	private JedisCluster cluster;
	private ClusterCommandExecutor clusterCommandExecutor;

	/**
	 * Constructs a new <code>RedisHelperJedisConnectionFactory</code> instance with default settings (default connection pooling, no
	 * shard information).
	 */
	public RedisHelperJedisConnectionFactory() {}

	/**
	 * Constructs a new <code>RedisHelperJedisConnectionFactory</code> instance. Will override the other connection parameters passed
	 * to the factory.
	 * 
	 * @param shardInfo shard information
	 */
	public RedisHelperJedisConnectionFactory(JedisShardInfo shardInfo) {
		this.shardInfo = shardInfo;
	}

	/**
	 * Constructs a new <code>RedisHelperJedisConnectionFactory</code> instance using the given pool configuration.
	 * 
	 * @param poolConfig pool configuration
	 */
	public RedisHelperJedisConnectionFactory(JedisPoolConfig poolConfig) {
		this((RedisSentinelConfiguration) null, poolConfig);
	}

	/**
	 * Constructs a new {@link RedisHelperJedisConnectionFactory} instance using the given {@link JedisPoolConfig} applied to
	 * {@link JedisSentinelPool}.
	 * 
	 * @param sentinelConfig
	 * @since 1.4
	 */
	public RedisHelperJedisConnectionFactory(RedisSentinelConfiguration sentinelConfig) {
		this(sentinelConfig, null);
	}

	/**
	 * Constructs a new {@link RedisHelperJedisConnectionFactory} instance using the given {@link JedisPoolConfig} applied to
	 * {@link JedisSentinelPool}.
	 * 
	 * @param sentinelConfig
	 * @param poolConfig pool configuration. Defaulted to new instance if {@literal null}.
	 * @since 1.4
	 */
	public RedisHelperJedisConnectionFactory(RedisSentinelConfiguration sentinelConfig, JedisPoolConfig poolConfig) {
		this.sentinelConfig = sentinelConfig;
		this.poolConfig = poolConfig != null ? poolConfig : new JedisPoolConfig();
	}

	/**
	 * Constructs a new {@link RedisHelperJedisConnectionFactory} instance using the given {@link RedisClusterConfiguration} applied
	 * to create a {@link JedisCluster}.
	 * 
	 * @param clusterConfig
	 * @since 1.7
	 */
	public RedisHelperJedisConnectionFactory(RedisClusterConfiguration clusterConfig) {
		this.clusterConfig = clusterConfig;
	}

	/**
	 * Constructs a new {@link RedisHelperJedisConnectionFactory} instance using the given {@link RedisClusterConfiguration} applied
	 * to create a {@link JedisCluster}.
	 * 
	 * @param clusterConfig
	 * @since 1.7
	 */
	public RedisHelperJedisConnectionFactory(RedisClusterConfiguration clusterConfig, JedisPoolConfig poolConfig) {
		this.clusterConfig = clusterConfig;
		this.poolConfig = poolConfig;
	}

	/**
	 * Returns a Jedis instance to be used as a Redis connection. The instance can be newly created or retrieved from a
	 * pool.
	 * 
	 * @return Jedis instance ready for wrapping into a {@link RedisConnection}.
	 */
	protected Jedis fetchJedisConnector() {
		try {

			if (usePool && pool != null) {
				return pool.getResource();
			}
			Jedis jedis = new Jedis(getShardInfo());
			// force initialization (see Jedis issue #82)
			jedis.connect();
			return jedis;
		} catch (Exception ex) {
			throw new RedisConnectionFailureException("Cannot get Jedis connection", ex);
		}
	}

	/**
	 * Post process a newly retrieved connection. Useful for decorating or executing initialization commands on a new
	 * connection. This implementation simply returns the connection.
	 * 
	 * @param connection
	 * @return processed connection
	 */
	protected RedisHelperJedisConnection postProcessConnection(RedisHelperJedisConnection connection) {
		return connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() {
		if (shardInfo == null) {
			shardInfo = new JedisShardInfo(hostName, port);

			if (StringUtils.hasLength(password)) {
				shardInfo.setPassword(password);
			}

			if (timeout > 0) {
				setTimeoutOn(shardInfo, timeout);
			}
		}

		if (usePool && clusterConfig == null) {
			this.pool = createPool();
		}

		if (clusterConfig != null) {
			this.cluster = createCluster();
		}
	}

	private Pool<Jedis> createPool() {

		if (isRedisSentinelAware()) {
			return createRedisSentinelPool(this.sentinelConfig);
		}
		return createRedisPool();
	}

	/**
	 * Creates {@link JedisSentinelPool}.
	 * 
	 * @param config
	 * @return
	 * @since 1.4
	 */
	protected Pool<Jedis> createRedisSentinelPool(RedisSentinelConfiguration config) {
		return new JedisSentinelPool(config.getMaster().getName(), convertToJedisSentinelSet(config.getSentinels()),
				getPoolConfig() != null ? getPoolConfig() : new JedisPoolConfig(), getTimeoutFrom(getShardInfo()),
				getShardInfo().getPassword());
	}

	/**
	 * Creates {@link RedisHelperJedisPool}.
	 * 
	 * @return
	 * @since 1.4
	 */
	protected Pool<Jedis> createRedisPool() {
		return new RedisHelperJedisPool(getPoolConfig(), getShardInfo().getHost(), getShardInfo().getPort(),
				getTimeoutFrom(getShardInfo()), getShardInfo().getPassword());
	}

	private JedisCluster createCluster() {

		JedisCluster cluster = createCluster(this.clusterConfig, this.poolConfig);
		this.clusterCommandExecutor = new ClusterCommandExecutor(new RedisHelperJedisClusterConnection.JedisClusterTopologyProvider(
				cluster), new RedisHelperJedisClusterConnection.JedisClusterNodeResourceProvider(cluster), EXCEPTION_TRANSLATION);
		return cluster;
	}

	/**
	 * Creates {@link JedisCluster} for given {@link RedisClusterConfiguration} and {@link GenericObjectPoolConfig}.
	 * 
	 * @param clusterConfig must not be {@literal null}.
	 * @param poolConfig can be {@literal null}.
	 * @return
	 * @since 1.7
	 */
	protected JedisCluster createCluster(RedisClusterConfiguration clusterConfig, GenericObjectPoolConfig poolConfig) {

		Assert.notNull("Cluster configuration must not be null!");

		Set<HostAndPort> hostAndPort = new HashSet<HostAndPort>();
		for (RedisNode node : clusterConfig.getClusterNodes()) {
			hostAndPort.add(new HostAndPort(node.getHost(), node.getPort()));
		}

		int redirects = clusterConfig.getMaxRedirects() != null ? clusterConfig.getMaxRedirects().intValue() : 5;

		if (StringUtils.hasText(getPassword())) {
			throw new IllegalArgumentException("Jedis does not support password protected Redis Cluster configurations!");
		}

		if (poolConfig != null) {
			return new JedisCluster(hostAndPort, timeout, redirects, poolConfig);
		}
		return new JedisCluster(hostAndPort, timeout, redirects, poolConfig);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	public void destroy() {
		if (usePool && pool != null) {
			try {
				pool.destroy();
			} catch (Exception ex) {
				log.warn("Cannot properly close Jedis pool", ex);
			}
			pool = null;
		}
		if (cluster != null) {
			try {
				cluster.close();
			} catch (Exception ex) {
				log.warn("Cannot properly close Jedis cluster", ex);
			}
			try {
				clusterCommandExecutor.destroy();
			} catch (Exception ex) {
				log.warn("Cannot properly close cluster command executor", ex);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redisHelper.connection.RedisConnectionFactory#getConnection()
	 */
	public RedisConnection getConnection() {

		if (cluster != null) {
			return getClusterConnection();
		}

		Jedis jedis = fetchJedisConnector();
		RedisHelperJedisConnection connection = (usePool ? new RedisHelperJedisConnection(jedis, pool, dbIndex) : new RedisHelperJedisConnection(jedis,
				null, dbIndex));
		connection.setConvertPipelineAndTxResults(convertPipelineAndTxResults);
		return postProcessConnection(connection);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redisHelper.connection.RedisConnectionFactory#getClusterConnection()
	 */
	@Override
	public RedisClusterConnection getClusterConnection() {

		if (cluster == null) {
			throw new InvalidDataAccessApiUsageException("Cluster is not configured!");
		}
		return new org.springframework.data.redis.connection.jedis.JedisClusterConnection(cluster, clusterCommandExecutor);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return EXCEPTION_TRANSLATION.translate(ex);
	}

	/**
	 * Returns the Redis hostName.
	 * 
	 * @return Returns the hostName
	 */
	public String getHostName() {
		return hostName;
	}

	/**
	 * Sets the Redis hostName.
	 * 
	 * @param hostName The hostName to set.
	 */
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	/**
	 * Returns the password used for authenticating with the Redis server.
	 * 
	 * @return password for authentication
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * Sets the password used for authenticating with the Redis server.
	 * 
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Returns the port used to connect to the Redis instance.
	 * 
	 * @return Redis port.
	 */
	public int getPort() {
		return port;

	}

	/**
	 * Sets the port used to connect to the Redis instance.
	 * 
	 * @param port Redis port
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * Returns the shardInfo.
	 * 
	 * @return Returns the shardInfo
	 */
	public JedisShardInfo getShardInfo() {
		return shardInfo;
	}

	/**
	 * Sets the shard info for this factory.
	 * 
	 * @param shardInfo The shardInfo to set.
	 */
	public void setShardInfo(JedisShardInfo shardInfo) {
		this.shardInfo = shardInfo;
	}

	/**
	 * Returns the timeout.
	 * 
	 * @return Returns the timeout
	 */
	public int getTimeout() {
		return timeout;
	}

	/**
	 * @param timeout The timeout to set.
	 */
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	/**
	 * Indicates the use of a connection pool.
	 * 
	 * @return Returns the use of connection pooling.
	 */
	public boolean getUsePool() {
		return usePool;
	}

	/**
	 * Turns on or off the use of connection pooling.
	 * 
	 * @param usePool The usePool to set.
	 */
	public void setUsePool(boolean usePool) {
		this.usePool = usePool;
	}

	/**
	 * Returns the poolConfig.
	 * 
	 * @return Returns the poolConfig
	 */
	public JedisPoolConfig getPoolConfig() {
		return poolConfig;
	}

	/**
	 * Sets the pool configuration for this factory.
	 * 
	 * @param poolConfig The poolConfig to set.
	 */
	public void setPoolConfig(JedisPoolConfig poolConfig) {
		this.poolConfig = poolConfig;
	}

	/**
	 * Returns the index of the database.
	 * 
	 * @return Returns the database index
	 */
	public int getDatabase() {
		return dbIndex;
	}

	/**
	 * Sets the index of the database used by this connection factory. Default is 0.
	 * 
	 * @param index database index
	 */
	public void setDatabase(int index) {
		Assert.isTrue(index >= 0, "invalid DB index (a positive index required)");
		this.dbIndex = index;
	}

	/**
	 * Specifies if pipelined results should be converted to the expected data type. If false, results of
	 * {@link org.springframework.data.redis.connection.jedis.JedisConnection#closePipeline()} and {@link org.springframework.data.redis.connection.jedis.JedisConnection#exec()} will be of the type returned by the
	 * Jedis driver
	 * 
	 * @return Whether or not to convert pipeline and tx results
	 */
	public boolean getConvertPipelineAndTxResults() {
		return convertPipelineAndTxResults;
	}

	/**
	 * Specifies if pipelined results should be converted to the expected data type. If false, results of
	 * {@link org.springframework.data.redis.connection.jedis.JedisConnection#closePipeline()} and {@link RedisHelperJedisConnection#exec()} will be of the type returned by the
	 * Jedis driver
	 * 
	 * @param convertPipelineAndTxResults Whether or not to convert pipeline and tx results
	 */
	public void setConvertPipelineAndTxResults(boolean convertPipelineAndTxResults) {
		this.convertPipelineAndTxResults = convertPipelineAndTxResults;
	}

	/**
	 * @return true when {@link RedisSentinelConfiguration} is present.
	 * @since 1.4
	 */
	public boolean isRedisSentinelAware() {
		return sentinelConfig != null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redisHelper.connection.RedisConnectionFactory#getSentinelConnection()
	 */
	@Override
	public RedisSentinelConnection getSentinelConnection() {

		if (!isRedisSentinelAware()) {
			throw new InvalidDataAccessResourceUsageException("No Sentinels configured");
		}

		return new JedisSentinelConnection(getActiveSentinel());
	}

	private Jedis getActiveSentinel() {

		Assert.notNull(this.sentinelConfig);
		for (RedisNode node : this.sentinelConfig.getSentinels()) {
			Jedis jedis = new Jedis(node.getHost(), node.getPort());
			if (jedis.ping().equalsIgnoreCase("pong")) {
				return jedis;
			}
		}

		throw new InvalidDataAccessResourceUsageException("no sentinel found");
	}

	private Set<String> convertToJedisSentinelSet(Collection<RedisNode> nodes) {

		if (CollectionUtils.isEmpty(nodes)) {
			return Collections.emptySet();
		}

		Set<String> convertedNodes = new LinkedHashSet<String>(nodes.size());
		for (RedisNode node : nodes) {
			if (node != null) {
				convertedNodes.add(node.asString());
			}
		}
		return convertedNodes;
	}

	private void setTimeoutOn(JedisShardInfo shardInfo, int timeout) {
		ReflectionUtils.invokeMethod(SET_TIMEOUT_METHOD, shardInfo, timeout);
	}

	private int getTimeoutFrom(JedisShardInfo shardInfo) {
		return (Integer) ReflectionUtils.invokeMethod(GET_TIMEOUT_METHOD, shardInfo);
	}

}
