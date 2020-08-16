package com.github.andyshaox.redis.lock;

import com.github.andyshao.lock.ExpireMode;
import com.github.andyshaox.redis.IntegrationTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import reactor.core.publisher.Mono;

@Slf4j
class RedisReactorDistributionLockTest extends IntegrationTest {
    @Autowired
    private ReactiveRedisConnectionFactory factory;

    @Test
    void unlock() {
    }

    @Test
    void lock() {
    }

    @Test
    void tryLock() {
        RedisReactorDistributionLock lock =
                new RedisReactorDistributionLock(this.factory, "GearEE-Redis:ReactorDistributionLock:tryLock");
        lock.tryLock(ExpireMode.SECONDS, 100)
                .flatMap(hasLock -> {
                    log.info("Is Getting the lock ? {}", hasLock);
                    if(hasLock) return lock.unlock();
                    return Mono.just(false);
                })
                .block();
        log.info("END");
    }
}