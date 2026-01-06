package io.braineous.dd.config;

import ai.braineous.cgo.config.ConfigService;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class ConfigServiceCdiIT {

    @Inject
    ConfigService config;

    @Test
    void configService_is_injected() {
        assertNotNull(config);
    }
}
