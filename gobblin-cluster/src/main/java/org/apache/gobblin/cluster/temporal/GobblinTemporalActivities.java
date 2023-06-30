// @@@SNIPSTART hello-world-project-template-java-activity-interface
package org.apache.gobblin.cluster.temporal;

import io.temporal.activity.ActivityInterface;

@ActivityInterface
public interface GobblinTemporalActivities {

    // Define your activity methods which can be called during workflow execution
    String composeGreeting(String name);

}
// @@@SNIPEND
