// @@@SNIPSTART hello-world-project-template-java-workflow-interface
package org.apache.gobblin.cluster.temporal;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface GobblinTemporalWorkflow {

    /**
     * This is the method that is executed when the Workflow Execution is started. The Workflow
     * Execution completes when this method finishes execution.
     */
    @WorkflowMethod
    String getGreeting(String name);
}
// @@@SNIPEND
