package com.github.andyshaox.redis.lock;

import com.github.andyshao.lock.ExpireMode;
import com.github.andyshaox.redis.IntegrationTest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;

class RedisReactiveRepeatCheckTest extends IntegrationTest {
    @Autowired
    private ReactiveRedisConnectionFactory factory;

    @Test
    void isRepeat() {
        RedisReactiveRepeatCheck repeatCheck = new RedisReactiveRepeatCheck(this.factory);
        final String uniqueKey = "GearEE-Redis:ReactiveRepeatCheck:isRepeat";
        repeatCheck.isRepeat(uniqueKey, ExpireMode.MILISECONDS, 200)
                .doOnNext(isRepeat -> Assertions.assertThat(isRepeat).isFalse())
                .then(repeatCheck.isRepeat(uniqueKey, ExpireMode.MILISECONDS, 200))
                .doOnNext(isRepeat -> Assertions.assertThat(isRepeat).isTrue())
                .block();
    }
}