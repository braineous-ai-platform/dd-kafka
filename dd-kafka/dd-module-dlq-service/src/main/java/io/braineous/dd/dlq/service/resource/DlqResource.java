package io.braineous.dd.dlq.service.resource;

import ai.braineous.rag.prompt.observe.Console;


public class DlqResource {

 public void handleSystemFailure(String payload){

  Console.log("system_exception", payload);
 }

 public void handleDomainFailure(String payload) {

  Console.log("domain_exception", payload);
 }
}

