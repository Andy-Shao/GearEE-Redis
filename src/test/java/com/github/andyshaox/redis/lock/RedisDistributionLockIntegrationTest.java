package com.github.andyshaox.redis.lock;

import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import com.github.andyshaox.redis.IntegrationTest;

import lombok.extern.slf4j.Slf4j;

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
        final CountDownLatch waitEnd = new CountDownLatch(2);
        final CountDownLatch left = new CountDownLatch(1);
        final CountDownLatch right = new CountDownLatch(1);
        Thread leftThread = new Thread(()->{
            try {
                lock.lock();
                lock.lock();
            } finally {
                lock.unlock();
                log.info("left countDown");
                left.countDown();
                try {
                    right.await();
                } catch (InterruptedException e) {
                    Assert.fail();
                }
                lock.unlock();
            }
            waitEnd.countDown();
        });
        Thread rightThread = new Thread(()->{
            try {
                try {
                    left.await();
                } catch (InterruptedException e) {
                    Assert.fail();
                }
                boolean hasLock = lock.tryLock();
                Assert.assertFalse(hasLock);
            } finally {
                lock.unlock();
                right.countDown();
                log.info("right countDown");
            }
            waitEnd.countDown();
        });
        leftThread.start();
        rightThread.start();
        
        waitEnd.await();
    }
}