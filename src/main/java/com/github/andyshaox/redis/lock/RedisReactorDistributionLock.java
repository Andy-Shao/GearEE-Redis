package com.github.andyshaox.redis.lock;

import com.github.andyshao.lock.ExpireMode;
import com.github.andyshao.lock.LockException;
import com.github.andyshao.lock.ReactorDistributionLock;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.types.Expiration;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Date;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Title: <br>
 * Description: <br>
 * Copyright: Copyright(c) 2020/8/15
 * Encoding: UNIX UTF-8
 *
 * @author Andy.Shao
 */
public class RedisReactorDistributionLock implements ReactorDistributionLock {
    public static final String DEFAULT_KEY = RedisDistributionLock.class.getName() + "_REACTOR_DISTRIBUTION_LOCK_KEY";
    private final ReactiveRedisConnectionFactory redisConnectionFactory;
    private final byte[] lockKey;
    private volatile byte[] lockValue;
    private final int sleepTime;
    private final TimeUnit sleepTimeUnit;
    private final LockOwner lockOwer = new LockOwner();

    public RedisReactorDistributionLock(ReactiveRedisConnectionFactory redisConnectionFactory) {
        this(redisConnectionFactory, DEFAULT_KEY);
    }

    public RedisReactorDistributionLock(ReactiveRedisConnectionFactory redisConnectionFactory, String lockKey) {
        this(redisConnectionFactory, lockKey.getBytes());
    }

    public RedisReactorDistributionLock(ReactiveRedisConnectionFactory redisConnectionFactory, byte[] lockKey) {
        this(redisConnectionFactory, lockKey, TimeUnit.NANOSECONDS, 100);
    }
    public RedisReactorDistributionLock(ReactiveRedisConnectionFactory redisConnectionFactory, byte[] lockKey,
                                        TimeUnit sleepTimeUnit, int sleepTime) {
        this.redisConnectionFactory = redisConnectionFactory;
        this.lockKey = lockKey;
        this.sleepTime = sleepTime;
        this.sleepTimeUnit = sleepTimeUnit;
    }

    @Override
    public Mono<Void> unlock() {
        final ReactiveRedisConnection conn = this.redisConnectionFactory.getReactiveConnection();
        return Mono.<Boolean>just(this.lockOwer.canUnlock())
                .flatMap(canUnLock -> {
                    if(canUnLock) {
                        return conn.keyCommands()
                                .del(ByteBuffer.wrap(this.lockKey))
                                .<Void>map(omitNum -> {
                                    if(omitNum <= 0) throw new LockException("Lock does no exists!");
                                    return null;
                                });
                    } else return Mono.<Void>empty();
                })
                .doFinally(signalType -> conn.close());
    }

    @Override
    public Mono<Void> lock() {
        return lock(ExpireMode.IGNORE, 1000);
    }

    @Override
    public Mono<Void> lock(ExpireMode mode, int times) {
        return tryLock(mode, times)
                .flatMap(hasLock -> {
                    if(!hasLock) {
                        switch (mode) {
                            case IGNORE:
                                return lock(mode, times);
                            default:
                                break;
                        }
                    }
                    return Mono.<Void>create(MonoSink::success);
                });
    }

    @Override
    public Mono<Boolean> tryLock() {
        return tryLock(ExpireMode.IGNORE, 1000);
    }

    @Override
    public Mono<Boolean> tryLock(ExpireMode expireMode, int expireTimes) {
        final ReactiveRedisConnection conn = this.redisConnectionFactory.getReactiveConnection();
        return tryAcquireLock(conn, expireMode, expireTimes)
                .flatMap(hasLock -> {
                    if(hasLock) {
                        return addExpireTime(conn, expireMode, expireTimes)
                                .map(isSuccess -> true);
                    }
                    return Mono.just(false);
                })
                .doFinally(signalType -> conn.close());
    }

    private static class LockSign {
        protected volatile Thread thread;
        protected volatile AtomicLong size = new AtomicLong(0L);
        protected volatile long timeSign = 0;

        protected LockSign copy() {
            LockSign ret = new LockSign();
            ret.thread = this.thread;
            ret.size = new AtomicLong(this.size.get());
            ret.timeSign = this.timeSign;
            return ret;
        }

        @Override
        public boolean equals(Object o) {
            if(Objects.isNull(o)) return false;
            if(o instanceof LockSign) {
                LockSign that = (LockSign) o;
                return Objects.equals(this.thread,  that.thread) && Objects.equals(this.size, that.size)
                        && Objects.equals(this.timeSign, that.timeSign);
            } else return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.thread, this.size, this.timeSign);
        }
    }

    private static class LockOwner {
        protected final AtomicReference<LockSign> lockSign;

        private LockOwner() {
            this.lockSign = new AtomicReference<>();
            this.lockSign.set(new LockSign());
        }

        public void setTimeSign(long timeSign) {
            for(;;) {
                LockSign ls = this.lockSign.get();
                LockSign newLs = ls.copy();
                ls.timeSign = timeSign;
                if(this.lockSign.compareAndSet(ls, newLs)) return;
            }
        }

        public boolean isOwner() {
            LockSign ls = this.lockSign.get();
            LockSign myLs = ls.copy();
            myLs.thread = Thread.currentThread();
            return this.lockSign.compareAndSet(myLs, ls);
        }

        public boolean increment() {
            for(;;) {
                LockSign ls = this.lockSign.get();
                LockSign newLs = ls.copy();
                if(Objects.isNull(ls.thread)) {
                    newLs.thread = Thread.currentThread();
                    newLs.size = new AtomicLong(0L);
                    if(!this.lockSign.compareAndSet(ls, newLs)) continue;
                    return false;
                } else {
                    newLs.size.incrementAndGet();
                    if(!this.lockSign.compareAndSet(ls, newLs)) continue;
                    return true;
                }
            }
        }

        public boolean canUnlock() {
            for(;;) {
                LockSign ls = this.lockSign.get();
                LockSign newLs = ls.copy();
                if(ls.timeSign <= new Date().getTime()) {
                    newLs.thread = null;
                    newLs.size = null;
                    if(!this.lockSign.compareAndSet(ls, newLs)) continue;
                    return false;
                } else if(Objects.equals(Thread.currentThread(), ls.thread)) {
                    if(ls.size.longValue() <= 0L) {
                        newLs.thread = null;
                        newLs.size = null;
                        if(!this.lockSign.compareAndSet(ls, newLs)) continue;
                        return true;
                    } else {
                        newLs.size.decrementAndGet();
                        if(!this.lockSign.compareAndSet(ls, newLs)) continue;
                        return false;
                    }
                }
            }
        }
    }

    private Mono<Boolean> tryAcquireLock(ReactiveRedisConnection conn, ExpireMode expireMode, int expireTimes) {
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
            return Mono.just(this.lockOwer.increment());
        }
        this.lockValue = (DEFAULT_KEY + String.valueOf(new Random().nextLong())).getBytes();
        Mono<Boolean> ret = conn.stringCommands().setNX(ByteBuffer.wrap(this.lockKey), ByteBuffer.wrap(this.lockValue));
        final long timeSign = l;
        return ret.doOnNext(status -> {
            if(status) {
                this.lockOwer.setTimeSign(timeSign);
                this.lockOwer.increment();
            }
        });
    }

    private Mono<Boolean> addExpireTime(ReactiveRedisConnection conn , ExpireMode expireMode , int expireTimes) {
        if(expireTimes < 0) throw new IllegalArgumentException();
        if(expireTimes == 0) return Mono.just(true);
        //设置锁的使用超时时间
        switch (expireMode) {
            case SECONDS:
                return conn.keyCommands().expire(ByteBuffer.wrap(this.lockKey), Duration.ofSeconds(expireTimes));
            case MILISECONDS:
                return conn.keyCommands().pExpire(ByteBuffer.wrap(this.lockKey), Duration.ofMillis(expireTimes));
            default:
                return Mono.just(true);
        }
    }
}
