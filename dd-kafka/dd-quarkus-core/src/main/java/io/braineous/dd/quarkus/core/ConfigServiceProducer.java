package io.braineous.dd.quarkus.core;

import ai.braineous.cgo.config.ConfigService;
import ai.braineous.cgo.config.FileBackedConfigService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

@ApplicationScoped
public class ConfigServiceProducer {

    @Produces
    @ApplicationScoped
    public ConfigService configService() {
        return new FileBackedConfigService();
    }
}

