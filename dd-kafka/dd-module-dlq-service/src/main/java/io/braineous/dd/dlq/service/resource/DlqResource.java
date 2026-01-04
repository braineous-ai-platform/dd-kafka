package io.braineous.dd.dlq.service.resource;

import ai.braineous.rag.prompt.observe.Console;
import io.braineous.dd.dlq.DlqProcessor;
import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;


@Path("/dlq")
public class DlqResource {

 @Inject
 private DlqProcessor processor;

 @POST
 @Path("/system_failure")
 public void handleSystemFailure(String payload){
   this.processor.handleSystemFailure(payload);
 }

 @POST
 @Path("/domain_failure")
 public void handleDomainFailure(String payload) {
   this.processor.handleDomainFailure(payload);
 }
}

