package com.github.andyshaox.redis.lock;

import com.github.andyshao.lock.ExpireMode;
import com.github.andyshao.lock.ReactorDistributionLockSign;
import com.github.andyshaox.redis.IntegrationTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Slf4j
class RedisReactorDistributionLockTest extends IntegrationTest {
    @Autowired
    private ReactiveRedisConnectionFactory factory;

    @Test
    void tryLock() {
        RedisReactorDistributionLock lock =
                new RedisReactorDistributionLock(this.factory, "GearEE-Redis:ReactorDistributionLock:tryLock");
        final ReactorDistributionLockSign lockSign = new ReactorDistributionLockSign(UUID.randomUUID());
        lock.tryLock(lockSign, ExpireMode.SECONDS, 100)
                .flatMap(hasLock -> {
                    log.info("Is Getting the lock ? {}", hasLock);
                    if(hasLock) return lock.unlockLater(lockSign);
                    return Mono.just(false);
                })
                .block();
        log.info("END");
    }
}