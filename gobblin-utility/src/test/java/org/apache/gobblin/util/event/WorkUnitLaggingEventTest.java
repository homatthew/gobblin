package org.apache.gobblin.util.event;

import com.typesafe.config.ConfigFactory;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class WorkUnitLaggingEventTest {

  @Test
  public void testAsList() {
    WorkUnitLaggingEvent event = new WorkUnitLaggingEvent(ConfigFactory.empty(), WorkUnitLaggingEventTest.class.getName());
    List<String> topicPartitions = Arrays.asList("topic-1", "topic-2", "other-topic-3");
    event.addMetadata(WorkUnitLaggingEvent.WORK_UNIT_LAGGING_TOPIC_PARTITION_KEY, topicPartitions.toString());
    Assert.assertEquals(event.getLaggingTopicPartitions(), topicPartitions);
  }
}
