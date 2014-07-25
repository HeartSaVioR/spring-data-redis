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
package org.springframework.data.redis.connection;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;

/**
 * @author Christoph Strobl
 * @since 1.4
 */
public abstract class AbstractRedisConnection implements RedisConnection {

	private RedisSentinelConfiguration sentinelConfiguration;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#getSentinelCommands()
	 */
	@Override
	public RedisSentinelCommands getSentinelCommands() {

		if (!hasRedisSentinelConfigured()) {
			throw new InvalidDataAccessResourceUsageException("No sentinels configured.");
		}

		return getSentinelCommands(selectActiveSentinel());
	}

	public void setSentinelConfiguration(RedisSentinelConfiguration sentinelConfiguration) {
		this.sentinelConfiguration = sentinelConfiguration;
	}

	public boolean hasRedisSentinelConfigured() {
		return this.sentinelConfiguration != null;
	}

	private RedisNode selectActiveSentinel() {

		for (RedisNode node : this.sentinelConfiguration.getSentinels()) {
			if (isActive(node)) {
				return node;
			}
		}

		throw new InvalidDataAccessApiUsageException("Could not find any active sentinels");
	}

	/**
	 * Check if node is active by sending ping.
	 * 
	 * @param node
	 * @return
	 */
	protected boolean isActive(RedisNode node) {
		return false;
	}

	/**
	 * Get {@link RedisSentinelCommands} connected to given node.
	 * 
	 * @param sentinel
	 * @return
	 */
	protected RedisSentinelCommands getSentinelCommands(RedisNode sentinel) {
		throw new UnsupportedOperationException("Sentinel is not supported by this client.");
	}

}
