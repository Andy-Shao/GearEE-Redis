package com.github.andyshaox.redis.lock;

import com.github.andyshao.lock.ExpireMode;
import com.github.andyshao.lock.ReactorDistributionLock;
import com.github.andyshao.lock.ReactorDistributionLockSign;
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
    public void unlock(ReactorDistributionLockSign sign) {
        this.unlockLater(sign).block();
    }

    @Override
    public Mono<Void> unlockLater(ReactorDistributionLockSign sign) {
        final ReactiveRedisConnection conn = this.redisConnectionFactory.getReactiveConnection();
        return Mono.<Boolean>just(this.lockOwer.canUnlock(sign))
                .<Void>flatMap(canUnLock -> {
                    if(canUnLock) {
                        return conn.keyCommands()
                                .del(ByteBuffer.wrap(this.lockKey))
                                .flatMap(omitNum -> Mono.create(MonoSink::success));
                    } else return Mono.create(MonoSink::success);
                })
                .doFinally(signalType -> conn.closeLater().subscribe());
    }

    @Override
    public Mono<Void> lock(ReactorDistributionLockSign sign) {
        return lock(sign, ExpireMode.IGNORE, 1000);
    }

    @Override
    public Mono<Void> lock(ReactorDistributionLockSign sign, ExpireMode mode, int times) {
        return tryLock(sign, mode, times)
                .flatMap(hasLock -> {
                    if(!hasLock) {
                        switch (mode) {
                            case IGNORE:
                                return lock(sign, mode, times);
                            default:
                                break;
                        }
                    }
                    return Mono.<Void>create(MonoSink::success);
                });
    }

    @Override
    public Mono<Boolean> tryLock(ReactorDistributionLockSign sign) {
        return tryLock(sign, ExpireMode.IGNORE, 1000);
    }

    @Override
    public Mono<Boolean> tryLock(ReactorDistributionLockSign sign, ExpireMode expireMode, int expireTimes) {
        final ReactiveRedisConnection conn = this.redisConnectionFactory.getReactiveConnection();
        return tryAcquireLock(sign, conn, expireMode, expireTimes)
                .flatMap(hasLock -> {
                    if(hasLock) {
                        return addExpireTime(conn, expireMode, expireTimes)
                                .map(isSuccess -> true);
                    }
                    return Mono.just(false);
                })
                .doFinally(signalType -> conn.closeLater().subscribe());
    }

    private static class LockSign {
        protected volatile ReactorDistributionLockSign sign;
        protected volatile AtomicLong size = new AtomicLong(0L);
        protected volatile long timeSign = 0;

        protected LockSign copy() {
            LockSign ret = new LockSign();
            ret.sign = this.sign;
            ret.size = new AtomicLong(this.size.get());
            ret.timeSign = this.timeSign;
            return ret;
        }

        @Override
        public boolean equals(Object o) {
            if(Objects.isNull(o)) return false;
            if(o instanceof LockSign) {
                LockSign that = (LockSign) o;
                return Objects.equals(this.sign,  that.sign) && Objects.equals(this.size, that.size)
                        && Objects.equals(this.timeSign, that.timeSign);
            } else return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.sign, this.size, this.timeSign);
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
                newLs.timeSign = timeSign;
                if(this.lockSign.compareAndSet(ls, newLs)) return;
            }
        }

        public boolean isOwner(ReactorDistributionLockSign sign) {
            LockSign ls = this.lockSign.get();
            LockSign myLs = ls.copy();
            myLs.sign = sign;
            return this.lockSign.compareAndSet(myLs, ls);
        }

        public boolean increment(ReactorDistributionLockSign sign) {
            for(;;) {
                LockSign ls = this.lockSign.get();
                LockSign newLs = ls.copy();
                if(Objects.isNull(ls.sign)) {
                    newLs.sign = sign;
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

        public boolean canUnlock(ReactorDistributionLockSign sign) {
            for(;;) {
                LockSign ls = this.lockSign.get();
                LockSign newLs = ls.copy();
                if(ls.timeSign <= new Date().getTime()) {
                    newLs.sign = null;
                    newLs.size = null;
                    if(!this.lockSign.compareAndSet(ls, newLs)) continue;
                    return false;
                } else if(Objects.equals(sign, ls.sign)) {
                    if(ls.size.longValue() <= 0L) {
                        newLs.sign = null;
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

    private Mono<Boolean> tryAcquireLock(ReactorDistributionLockSign sign, ReactiveRedisConnection conn,
                                         ExpireMode expireMode, int expireTimes) {
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
        if(this.lockOwer.isOwner(sign)) {
            this.lockOwer.setTimeSign(l);
            return Mono.just(this.lockOwer.increment(sign));
        }
        this.lockValue = (DEFAULT_KEY + String.valueOf(new Random().nextLong())).getBytes();
        Mono<Boolean> ret = conn.stringCommands().setNX(ByteBuffer.wrap(this.lockKey), ByteBuffer.wrap(this.lockValue));
        final long timeSign = l;
        return ret.doOnNext(status -> {
            if(status) {
                this.lockOwer.setTimeSign(timeSign);
                this.lockOwer.increment(sign);
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
