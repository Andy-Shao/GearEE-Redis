package com.github.andyshaox.redis;

import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest
@RunWith(SpringRunner.class)
@IfProfileValue(name = IntegrationTest.name, value = IntegrationTest.value)
public abstract class IntegrationTest {
    public static final String name = "integration.test";
    public static final String value = "true";
}
