/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.springframework.data.redis.connection.NamedNode;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.RedisServer;
import org.springframework.util.Assert;

import redis.clients.jedis.Jedis;

/**
 * @author Christoph Strobl
 * @since 1.4
 */
public class JedisSentinelConnection implements RedisSentinelConnection, Closeable {

	private Jedis jedis;

	public JedisSentinelConnection(RedisNode sentinel) {
		this(sentinel.getHost(), sentinel.getPort());
	}

	public JedisSentinelConnection(String host, int port) {
		this(new Jedis(host, port));
	}

	public JedisSentinelConnection(Jedis jedis) {

		Assert.notNull(jedis, "Cannot created JedisSentinelConnection using 'null' as client.");
		this.jedis = jedis;
		init();
	}

	@Override
	public void failover(NamedNode master) {

		Assert.notNull(master, "Redis node master must not be 'null' for failover.");
		Assert.hasText(master.getName(), "Redis master name must not be 'null' or empty for failover.");
		jedis.sentinelFailover(master.getName());

	}

	@Override
	public List<RedisServer> masters() {
		return JedisConverters.toListOfRedisServer(jedis.sentinelMasters());
	}

	@Override
	public void close() throws IOException {
		jedis.close();
	}

	private void init() {
		if (!jedis.isConnected()) {
			doInit(jedis);
		}
	}

	protected void doInit(Jedis jedis) {
		jedis.connect();
	}

}
