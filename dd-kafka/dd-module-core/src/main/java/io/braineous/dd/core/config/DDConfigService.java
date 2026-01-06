package io.braineous.dd.core.config;

import ai.braineous.cgo.config.ConfigService;
import ai.braineous.cgo.config.FileBackedConfigService;

public class DDConfigService {

    public ConfigService configService() {
        return new FileBackedConfigService();
    }
}
