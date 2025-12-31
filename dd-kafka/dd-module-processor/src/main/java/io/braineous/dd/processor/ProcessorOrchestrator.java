package io.braineous.dd.processor;

public class ProcessorOrchestrator {
    private static final ProcessorOrchestrator orchestrator = new ProcessorOrchestrator();

    private ProcessorOrchestrator() {
    }

    public static ProcessorOrchestrator getInstance(){
        return orchestrator;
    }

    public void orchestrate() throws Exception{

    }
}
