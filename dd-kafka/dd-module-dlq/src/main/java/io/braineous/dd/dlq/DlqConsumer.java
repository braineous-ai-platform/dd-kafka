package io.braineous.dd.dlq;

import ai.braineous.rag.prompt.observe.Console;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;


@ApplicationScoped
public class DlqConsumer {

 @Incoming("dead_letter_system")
 public void handleSystemFailure(String payload){
   Console.log("system_exception", payload);
 }

 @Incoming("dead_letter_domain")
 public void handleDomainFailure(String payload) {
  Console.log("domain_exception", payload);
 }
}

