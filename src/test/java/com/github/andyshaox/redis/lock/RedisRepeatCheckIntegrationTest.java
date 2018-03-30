package com.github.andyshaox.redis.lock;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import com.github.andyshao.lock.ExpireMode;
import com.github.andyshaox.redis.IntegrationTest;

public class RedisRepeatCheckIntegrationTest extends IntegrationTest {
    @Autowired
    private RedisConnectionFactory connectFactory;
    
    @Test
    public void testRepeatCheck() throws InterruptedException {
        RedisRepeatCheck repeatCheck = new RedisRepeatCheck(connectFactory);
        boolean isRepeat = repeatCheck.isRepeat("GearEE-Redis:RepeatCheck:testRepeatCheck" , ExpireMode.MILISECONDS , 200);
        Assert.assertFalse(isRepeat);
        isRepeat = repeatCheck.isRepeat("GearEE-Redis:RepeatCheck:testRepeatCheck");
        Assert.assertTrue(isRepeat);
        TimeUnit.SECONDS.sleep(1);
        isRepeat = repeatCheck.isRepeat("GearEE-Redis:RepeatCheck:testRepeatCheck" , ExpireMode.MILISECONDS , 200);
    }
}
