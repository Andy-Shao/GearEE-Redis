package com.github.andyshaox.redis.lock;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import com.github.andyshao.lock.DistributionLock;
import com.github.andyshao.lock.ExpireMode;
import com.github.andyshao.lock.LockException;
import com.github.andyshao.lock.RepeatCheck;

/**
 * 
 * Title:<br>
 * Descript:<br>
 * Copyright: Copryright(c) 18 Apr 2017<br>
 * Encoding:UNIX UTF-8
 * @author Andy.Shao
 *
 */
public class RedisRepeatCheck implements RepeatCheck {
    private final RedisConnectionFactory connFactory;
    
    public RedisRepeatCheck(RedisConnectionFactory connFactory) {
        this.connFactory = connFactory;
    }

    @Override
    public boolean isRepeat(String uniqueKey , ExpireMode mode , int times) {
        boolean result = false;
        RedisConnection conn = null;
        try {
            conn = this.connFactory.getConnection();
            result = this.isRepeat(conn , uniqueKey);
            if(result) this.setExpire(conn , uniqueKey , mode , times);
        } finally {
            if(conn != null) conn.close();
        }
        return result;
    }

    @Override
    public boolean isRepeat(String uniqueKey) {
        return this.isRepeat(uniqueKey, ExpireMode.SECONDS, 5 * 60);
    }

    @Override
    public DistributionLock repeatCheckLock(String uniqueKey , ExpireMode mode , int times) {
        return new DistributionLock() {
            private DistributionLock proxied = new RedisDistributionLock(connFactory, md5Key(uniqueKey));
            
            @Override
            public void unlock() {
                this.proxied.unlock();
            }
            
            @Override
            public boolean tryLock(ExpireMode expireMode , int expireTimes) {
                return this.proxied.tryLock(expireMode , expireTimes);
            }
            
            @Override
            public boolean tryLock() {
                return this.proxied.tryLock(mode , times);
            }
            
            @Override
            public void lockInterruptibly(ExpireMode expireMode , int expireTimes) throws InterruptedException {
                this.proxied.lockInterruptibly(expireMode , expireTimes);
            }
            
            @Override
            public void lockInterruptibly() throws InterruptedException {
                this.proxied.lockInterruptibly(mode , times);
            }
            
            @Override
            public void lock(ExpireMode expireMode , int expireTimes) {
                this.proxied.lock(expireMode , expireTimes);
            }
            
            @Override
            public void lock() {
                this.proxied.lock(mode, times);
            }
        };
    }

    private static byte[] md5Key(String key) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            return md5.digest(key.getBytes());
        } catch (NoSuchAlgorithmException e) {
            throw new LockException(e);
        }
    }
    
    private boolean isRepeat(RedisConnection conn, String key) {
        return !conn.setNX(md5Key(key), md5Key(key));
    }
    
    private boolean setExpire(RedisConnection conn, String key, ExpireMode mode, int times) {
        final byte[] keyBytes = md5Key(key);
        switch (mode) {
        case SECONDS:
            return conn.expire(keyBytes, times);
        case MILISECONDS:
            return conn.pExpire(keyBytes, times);
        default:
            return false;
        }
    }
}
