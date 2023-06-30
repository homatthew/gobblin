// @@@SNIPSTART hello-world-project-template-java-activity
package org.apache.gobblin.cluster.temporal;

public class GobblinTemporalActivitiesImpl implements GobblinTemporalActivities {

    @Override
    public String composeGreeting(String name) {
        return "Hello " + name + "!";
    }

}
// @@@SNIPEND
