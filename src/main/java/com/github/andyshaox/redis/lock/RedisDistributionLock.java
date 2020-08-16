package com.github.andyshaox.redis.lock;

import com.github.andyshao.lock.DistributionLock;
import com.github.andyshao.lock.ExpireMode;
import com.github.andyshao.lock.LockException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.types.Expiration;

import java.util.Date;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * Title:<br>
 * Descript:<br>
 * Copyright: Copryright(c) 18 Apr 2017<br>
 * Encoding:UNIX UTF-8
 * 
 * @author Andy.Shao
 *
 */
public class RedisDistributionLock implements DistributionLock {
    public static final String DEFAULT_KEY = "DISTRIBUTION_LOCK_KEY";
    private final RedisConnectionFactory connFactory;
    private final byte[] lockKey;
    private volatile byte[] lockValue;
    private final int sleepTime;
    private final TimeUnit sleepTimeUnit;
    private final RedisDistributionLock.LockOwer lockOwer = this.new LockOwer();
    
    private class LockOwer {
        private volatile Thread thread;
        private volatile AtomicLong size;
        private volatile long timeSign = 0;
        
        public void setTimeSign(long timeSign) {
            this.timeSign = timeSign;
        }

        public synchronized boolean isOwner() {
                return Objects.equals(this.thread, Thread.currentThread());
        }
        
        public synchronized boolean increment() {
            if(thread == null) {
                this.thread = Thread.currentThread();
                this.size = new AtomicLong(0L);
                return false;
            } else {
                this.size.incrementAndGet();
                return true;
            }
        }
        
        public synchronized boolean canUnlock() {
                if(this.timeSign <= new Date().getTime()) {
                    this.thread = null;
                    this.size = null;
                    return false;
                } else if(Objects.equals(Thread.currentThread() , this.thread)) {
                if(this.size.longValue() <= 0L) {
                    this.thread = null;
                    this.size = null;
                    return true;
                } else {
                    this.size.decrementAndGet();
                    return false;
                }
            } else return false;
        }
    }
    
    public RedisDistributionLock(RedisConnectionFactory connFactory) {
        this(connFactory , RedisDistributionLock.DEFAULT_KEY);
    }

    public RedisDistributionLock(RedisConnectionFactory connFactory , byte[] lockKey) {
        this(connFactory , lockKey , TimeUnit.NANOSECONDS , 100);
    }

    public RedisDistributionLock(RedisConnectionFactory connFactory , byte[] lockKey , TimeUnit sleepTimeUnit , int sleepTime) {
        this.lockKey = lockKey;
        this.connFactory = connFactory;
        this.sleepTimeUnit = sleepTimeUnit;
        this.sleepTime = sleepTime;
    }

    public RedisDistributionLock(RedisConnectionFactory connFactory , String lockKey) {
        this(connFactory , lockKey.getBytes());
    }

    public RedisDistributionLock(RedisConnectionFactory connFactory , String lockKey , TimeUnit sleepTimeUnit , int sleepTime) {
        this(connFactory , lockKey.getBytes() , sleepTimeUnit , sleepTime);
    }

    private void addExpireTime(RedisConnection conn , ExpireMode expireMode , int expireTimes) {
        if(expireTimes <= 0) return;
        //设置锁的使用超时时间
        switch (expireMode) {
        case SECONDS:
            conn.expire(this.lockKey , expireTimes);
            break;
        case MILISECONDS:
            conn.pExpire(this.lockKey , expireTimes);
            break;
        default:
            break;
        }
    }

    @Override
    public void lock() {
        this.lock(ExpireMode.IGNORE , -1);
    }

    @Override
    public void lock(ExpireMode expireMode , int expireTimes) {
        RedisConnection conn = null;
        try {
            conn = this.connFactory.getConnection();
            boolean hasLock = true;
            while (!this.tryAcquireLock(conn, expireMode, expireTimes))
                try {
                    switch (expireMode) {
                        case IGNORE:
                            TimeUnit.MILLISECONDS.sleep(100);
                            continue;
                        default:
                            hasLock = false;
                            break;
                    }
                } catch (InterruptedException ex) {
                    throw new LockException(ex);
                }
            if(hasLock) this.addExpireTime(conn , expireMode , expireTimes);
        } finally {
            if (conn != null) conn.close();
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        this.lockInterruptibly(ExpireMode.IGNORE , -1);
    }

    @Override
    public void lockInterruptibly(ExpireMode expireMode , int expireTimes) throws InterruptedException {
        RedisConnection conn = null;
        try {
            conn = this.connFactory.getConnection();
            boolean hasLock = true;
            //间隔一段时间获取锁，直到获取
            while (!this.tryAcquireLock(conn, expireMode, expireTimes)) {
                if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
                switch (expireMode) {
                    case IGNORE:
                        TimeUnit.MILLISECONDS.sleep(100);
                        continue;
                    default:
                        hasLock = false;
                        break;
                }
            }
            if(hasLock) this.addExpireTime(conn , expireMode , expireTimes);
        } finally {
            if (conn != null) conn.close();
        }
    }

    private void sleep() throws InterruptedException {
        this.sleepTimeUnit.sleep(this.sleepTime);
    }

    private boolean tryAcquireLock(RedisConnection conn, ExpireMode expireMode, int expireTimes) {
        long l = new Date().getTime();
        Expiration expiration = null;
        switch(expireMode) {
        case MILISECONDS:
            l = l + expireTimes;
            expiration = Expiration.from(expireTimes, TimeUnit.MILLISECONDS);
            break;
        case SECONDS:
            l = l + (expireTimes * 1000);
            expiration = Expiration.from(expireTimes, TimeUnit.SECONDS);
            break;
                
        case IGNORE:
        default :
        	l = Long.MAX_VALUE;
        	expiration = Expiration.persistent();
            break;
        }
        if(this.lockOwer.isOwner()) {
            this.lockOwer.setTimeSign(l);
            return this.lockOwer.increment();
        }
        this.lockValue = (DEFAULT_KEY + String.valueOf(new Random().nextLong())).getBytes();
//        Boolean result = conn.set(this.lockKey, this.lockValue, expiration, SetOption.SET_IF_ABSENT);
        Boolean result = conn.setNX(this.lockKey, this.lockValue);
        if(result) {
            this.lockOwer.setTimeSign(l);
            this.lockOwer.increment();
        }
        return result;
    }

    @Override
    public boolean tryLock() {
        return this.tryLock(ExpireMode.IGNORE , -1);
    }

    @Override
    public boolean tryLock(ExpireMode expireMode , int expireTimes) {
        RedisConnection conn = null;
        try {
            conn = this.connFactory.getConnection();
            boolean result = this.tryAcquireLock(conn, expireMode, expireTimes);
            this.addExpireTime(conn, expireMode, expireTimes);
            return result;
        } finally {
            if (conn != null) conn.close();
        }
    }

    @Override
    public void unlock() {
        if(this.lockOwer.canUnlock()) {
            RedisConnection conn = null;
            try {
                conn = this.connFactory.getConnection();
                byte[] value = conn.get(this.lockKey);
                if(!Objects.deepEquals(value, this.lockValue)) throw new LockException("Lock does not exists!");
                conn.del(this.lockKey);
            } finally {
                if (conn != null) conn.close();
            }
        }
    }
}
