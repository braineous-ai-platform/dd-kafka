package io.braineous.dd.core.config;

import ai.braineous.cgo.config.ConfigService;
import ai.braineous.cgo.config.FileBackedConfigService;

public class DDConfigService {

    public static final String dd_http_port = "dd.http.port";
    public static final String dd_env = "dd.env";

    public ConfigService configService() {
        return new FileBackedConfigService();
    }

    public String internalProducerBase(String ddEnv) {
        if (ddEnv == null) ddEnv = "local";

        int ddPackPort = Integer.parseInt(this.configService().getProperty(dd_http_port));

        return switch (ddEnv) {
            case "local"  -> "http://localhost:8081";
            case "docker" -> "http://dd-module-kafka-producer:8081"; // same today, diverge later
            case "test"   -> "http://localhost:" + ddPackPort + "/__internal/producer";
            default       -> "http://localhost:8081";
        };
    }

    public String internalDlqBase(String ddEnv) {
        if (ddEnv == null) ddEnv = "local";

        int ddPackPort = Integer.parseInt(this.configService().getProperty(dd_http_port));

        return switch (ddEnv) {
            case "local"  -> "http://localhost:8083";
            case "docker" -> "http://dd-module-dlq-service:8083"; // same today, diverge later
            case "test"   -> "http://localhost:" + ddPackPort + "/__internal/dlq";
            default       -> "http://localhost:8083";
        };
    }


}
