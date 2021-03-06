package com.github.andyshaox.redis.lock;

import com.github.andyshaox.redis.IntegrationTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnectionFactory;

@Slf4j
public class RedisDistributionLockIntegrationTest extends IntegrationTest {
    @Autowired
    private RedisConnectionFactory connectFactory;
    
    @Test
    public void testTryLock() {
        RedisDistributionLock lock = new RedisDistributionLock(connectFactory , "GearEE-Redis:DistributionLock:testTryLock");
        try {
            lock.tryLock();
        } finally {
            lock.unlock();
        }
    }
    
    @Test
    public void testMixLock() throws InterruptedException {
        final RedisDistributionLock lock = new RedisDistributionLock(connectFactory, "GearEE-Redis:DistributionLock:testMixLock");
        try {
            lock.lock();
            lock.lock();
        } finally {
            lock.unlock();
            lock.unlock();
        }
    }
}
