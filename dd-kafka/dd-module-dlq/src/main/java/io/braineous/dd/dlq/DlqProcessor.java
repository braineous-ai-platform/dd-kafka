package io.braineous.dd.dlq;

import ai.braineous.rag.prompt.observe.Console;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class DlqProcessor {

    @Inject
    @Channel("dead_letter_system_out")
    Emitter<String> systemOut;

    @Inject
    @Channel("dead_letter_domain_out")
    Emitter<String> domainOut;

    @Incoming("dead_letter_system_in")
    public void handleSystemFailure(String payload){
        Console.log("system_exception", payload);
        systemOut.send(payload);
    }

    @Incoming("dead_letter_domain_in")
    public void handleDomainFailure(String payload) {
        Console.log("domain_exception", payload);
        domainOut.send(payload);
    }
}

