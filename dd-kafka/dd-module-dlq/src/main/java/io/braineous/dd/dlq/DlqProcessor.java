package io.braineous.dd.dlq;

import ai.braineous.rag.prompt.observe.Console;

import io.braineous.dd.dlq.persistence.DLQStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class DlqProcessor {

    @Inject
    private DLQStore store;

    @Inject
    @Channel("dlq_system_out")
    private Emitter<String> systemOut;

    @Inject
    @Channel("dlq_domain_out")
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

    @Incoming("dlq_system_in")
    public void consumeSystemFailure(String payload){
        try {
            Console.log("system_failure_consume", payload);
            store.storeSystemFailure(payload);
        }catch(Exception e){
            Console.log("error_processing_dlq_system_failure_while_storing", String.valueOf(e));
        }
    }

    @Incoming("dlq_domain_in")
    public void consumeDomainFailure(String payload){
        try {
            Console.log("domain_failure_consume", payload);
            store.storeDomainFailure(payload);
        }catch(Exception e){
            Console.log("error_processing_dlq_domain_failure_while_storing", String.valueOf(e));
        }
    }
}

