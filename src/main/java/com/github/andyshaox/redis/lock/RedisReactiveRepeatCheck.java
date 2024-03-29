package com.github.andyshaox.redis.lock;

import com.github.andyshao.lock.ExpireMode;
import com.github.andyshao.lock.ReactiveRepeatCheck;
import lombok.Setter;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Random;

/**
 * Title: <br>
 * Description: <br>
 * Copyright: Copyright(c) 2020/8/16
 * Encoding: UNIX UTF-8
 *
 * @author Andy.Shao
 */
public class RedisReactiveRepeatCheck implements ReactiveRepeatCheck {
    private final ReactiveRedisConnectionFactory factory;
    public static final String DEFAULT_REPEACH_CHECK_HEAD = "RedisReactorRepeatCheck:";
    @Setter
    private volatile String repeatCheckHead = DEFAULT_REPEACH_CHECK_HEAD;

    public RedisReactiveRepeatCheck(ReactiveRedisConnectionFactory factory) {
        this.factory = factory;
    }

    @Override
    public Mono<Boolean> isRepeat(String uniqueKey, ExpireMode mode, int times) {
        final ReactiveRedisConnection conn = this.factory.getReactiveConnection();
        return this.isRepeat(conn, uniqueKey, mode, times)
                .doFinally(signalType -> conn.closeLater().subscribe());
    }

    @Override
    public Mono<Boolean> isRepeat(String uniqueKey) {
        return this.isRepeat(uniqueKey, ExpireMode.SECONDS, 5 * 60);
    }

    private Mono<Boolean> isRepeat(ReactiveRedisConnection conn, String key, ExpireMode mode, int times) {
        final byte[] md5Value = RedisRepeatCheck.md5Key(key + new Random().nextLong());
        final byte[] md5Key = buildKey(key);
        return conn.stringCommands().setNX(ByteBuffer.wrap(md5Key), ByteBuffer.wrap(md5Value))
                .map(isSuccess -> !isSuccess)
                .flatMap(isRepeat -> {
                    if(!isRepeat) {
                        return setExpire(conn, md5Key, mode, times)
                                .map(isSet -> false);
                    }
                    return Mono.just(true);
                });
    }

    private byte[] buildKey(String key) {
        byte[] head = this.repeatCheckHead.getBytes();
        byte[] body = RedisRepeatCheck.md5Key(key);
        byte[] rest = new byte[head.length + body.length];
        System.arraycopy(head, 0, rest, 0, head.length);
        System.arraycopy(body, 0, rest, head.length, body.length);

        return rest;
    }

    private Mono<Boolean> setExpire(ReactiveRedisConnection conn, byte[] key, ExpireMode mode, int times) {
        if(times <= 0) return Mono.just(false);
        switch (mode) {
            case SECONDS:
                return conn.keyCommands().expire(ByteBuffer.wrap(key), Duration.ofSeconds(times));
            case MILLISECONDS:
                return conn.keyCommands().pExpire(ByteBuffer.wrap(key), Duration.ofMillis(times));
            default:
                return Mono.just(false);
        }
    }
}
