package com.github.andyshaox.redis.lock;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.core.types.Expiration;

import com.github.andyshao.lang.ByteOperation;
import com.github.andyshao.lock.DistributionLock;
import com.github.andyshao.lock.ExpireMode;
import com.github.andyshao.lock.LockException;
import com.github.andyshao.lock.RepeatCheck;

import lombok.Setter;

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
    public static final String DEFAULT_REPEACH_CHECK_HEAD = "RedisRepeatCheck:";
    @Setter
    private volatile String repeatCheckHead = DEFAULT_REPEACH_CHECK_HEAD;
    
    public RedisRepeatCheck(RedisConnectionFactory connFactory) {
        this.connFactory = connFactory;
    }

    @Override
    public boolean isRepeat(String uniqueKey , ExpireMode mode , int times) {
        boolean result = false;
        RedisConnection conn = null;
        try {
            conn = this.connFactory.getConnection();
//            result = this.isRepeat(conn , uniqueKey);
//            if(!result) this.setExpire(conn , uniqueKey , mode , times);
            result = this.isRepeat(conn, uniqueKey, mode, times);
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
    
    boolean isRepeat(RedisConnection conn, String key) {
//        return !conn.setNX(md5Key(key), md5Key(key));
    	return isRepeat(conn, key, ExpireMode.SECONDS, 5 * 60);
    }
    
    boolean isRepeat(RedisConnection conn, String key, ExpireMode mode, int times) {
    	Expiration expiration = null;
    	switch (mode) {
		case SECONDS:
			expiration = Expiration.seconds(times);
			break;
		case MILISECONDS:
			expiration = Expiration.milliseconds(times);
			break;
		case IGNORE:
		default:
			expiration = Expiration.persistent();
			break;
		}
    	return !conn.set(buildKey(key), md5Key(key + new Random().nextLong()), expiration, SetOption.SET_IF_ABSENT);
    }
    
    byte[] buildKey(String key) {
    	byte[] head = this.repeatCheckHead.getBytes();
    	byte[] body = md5Key(key);
    	byte[] rest = new byte[head.length + body.length];
    	System.arraycopy(head, 0, rest, 0, head.length);
    	System.arraycopy(body, 0, rest, head.length, body.length);
    	
    	return rest;
    }
    
	boolean setExpire(RedisConnection conn, String key, ExpireMode mode, int times) {
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
