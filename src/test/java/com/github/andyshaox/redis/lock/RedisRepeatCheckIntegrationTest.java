package com.github.andyshaox.redis.lock;

import com.github.andyshao.lock.ExpireMode;
import com.github.andyshaox.redis.IntegrationTest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import java.util.concurrent.TimeUnit;

public class RedisRepeatCheckIntegrationTest extends IntegrationTest {
    @Autowired
    private RedisConnectionFactory connectFactory;
    
    @Test
    public void testRepeatCheck() throws InterruptedException {
        RedisRepeatCheck repeatCheck = new RedisRepeatCheck(connectFactory);
        boolean isRepeat = repeatCheck.isRepeat("GearEE-Redis:RepeatCheck:testRepeatCheck" , ExpireMode.MILISECONDS , 200);
        Assertions.assertThat(isRepeat).isFalse();
        isRepeat = repeatCheck.isRepeat("GearEE-Redis:RepeatCheck:testRepeatCheck");
        Assertions.assertThat(isRepeat).isTrue();
        TimeUnit.SECONDS.sleep(1);
        isRepeat = repeatCheck.isRepeat("GearEE-Redis:RepeatCheck:testRepeatCheck" , ExpireMode.MILISECONDS , 200);
    }
}
