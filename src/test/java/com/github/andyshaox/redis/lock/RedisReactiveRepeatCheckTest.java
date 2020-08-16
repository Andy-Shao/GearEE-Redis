package com.github.andyshaox.redis.lock;

import com.github.andyshaox.redis.IntegrationTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;

class RedisReactiveRepeatCheckTest extends IntegrationTest {
    @Autowired
    private ReactiveRedisConnectionFactory factory;

}