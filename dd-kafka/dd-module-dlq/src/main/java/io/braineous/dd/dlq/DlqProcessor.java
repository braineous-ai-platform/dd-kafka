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
    private Emitter<String> systemOut;

    @Inject
    @Channel("dead_letter_domain_out")
    private Emitter<String> domainOut;

    public Emitter<String> getSystemOut() {
        return systemOut;
    }

    public Emitter<String> getDomainOut() {
        return domainOut;
    }

    public void handleSystemFailure(String payload){
        if(payload == null || payload.trim().length() == 0){
            return;
        }

        Console.log("system_exception_emit", payload);
        systemOut.send(payload);
    }


    public void handleDomainFailure(String payload) {
        if(payload == null || payload.trim().length() == 0){
            return;
        }

        Console.log("domain_exception_emit", payload);
        domainOut.send(payload);
    }
}

