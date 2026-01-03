package io.braineous.dd.dlq;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;


@ApplicationScoped
public class DlqConsumer {

 @Incoming("dead_letter.system.in")
 public void handleSystemFailure(String payload){
 }

 @Incoming("dead_letter.domain.in")
 public void handleDomainFailure(String payload) { }
}

