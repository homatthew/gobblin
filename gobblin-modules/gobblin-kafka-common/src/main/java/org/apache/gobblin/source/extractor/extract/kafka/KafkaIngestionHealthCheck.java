/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gobblin.source.extractor.extract.kafka;

import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.event.ContainerHealthCheckFailureEvent;
import org.apache.gobblin.util.event.MetadataBasedEvent;
import org.apache.gobblin.util.event.WorkUnitLaggingEvent;
import org.apache.gobblin.util.eventbus.EventBusFactory;


@Slf4j
@Alias(value = "KafkaIngestionHealthCheck")
public class KafkaIngestionHealthCheck implements CommitStep {
  private final Config config;
  private final EventBus eventBus;
  private final KafkaHealthModel healthModel;
  private final boolean dynamicWorkUnitEnabled;

  public KafkaIngestionHealthCheck(Config config, KafkaExtractorStatsTracker statsTracker) {
    this.config = config;
    this.healthModel = new KafkaHealthModel(config, statsTracker);
    this.dynamicWorkUnitEnabled = ConfigUtils.getBoolean(config, "isDynamicWorkUnitEnabled", true);
    this.eventBus = dynamicWorkUnitEnabled ?
        getEventBus(WorkUnitLaggingEvent.WORK_UNIT_LAGGING_EVENT_BUS_NAME) :
        getEventBus(ContainerHealthCheckFailureEvent.CONTAINER_HEALTH_CHECK_EVENT_BUS_NAME);
  }

  /**
   * Determine whether the commit step has been completed.
   */
  @Override
  public boolean isCompleted()
      throws IOException {
    return false;
  }

  /**
   * Execute the commit step. The execute method gets the maximum ingestion latency and the consumption rate and emits
   * a {@link ContainerHealthCheckFailureEvent} if the following conditions are satisfied:
   * <li>
   *   <ul>The ingestion latency increases monotonically over the {@link KafkaHealthModel#getSlidingWindowSize()} intervals, AND </ul>
   *   <ul>The maximum consumption rate over the {@link KafkaHealthModel#getSlidingWindowSize()} intervals is smaller than
   *   {@link KafkaHealthModel#getConsumptionRateDropOffFraction()} * {@link KafkaHealthModel#getExpectedConsumptionRate()}</ul>.
   * </li>
   *
   * The {@link ContainerHealthCheckFailureEvent} is posted to a global event bus. The handlers of this event type
   * can perform suitable actions based on the execution environment.
   */
  @Override
  public void execute() {
    healthModel.update();
    if (!healthModel.hasEnoughData()) {
      log.info("SUCCESS: Num observations: {} smaller than {}", healthModel.getNumberOfObservations(), healthModel.getSlidingWindowSize());
      return;
    }

    if (healthModel.isIngestionLatencyHealthy()) {
      log.info("SUCCESS: Ingestion Latencies = {}, Ingestion Latency Threshold: {}", healthModel.getIngestionLatencies().toString(), healthModel
          .getIngestionLatencyThresholdMinutes());
      return;
    }

    if (healthModel.isConsumptionRateHealthy()) {
      log.info("SUCCESS: Avg. Consumption Rate = {} MBps, Target Consumption rate = {} MBps", healthModel.getMaxConsumptionRate(), healthModel
          .getExpectedConsumptionRate());
      return;
    }

    log.error("FAILED: {}", healthModel.getHealthCheckReport());
    onFailure();
  }

  private void onFailure() {
    if (dynamicWorkUnitEnabled) {
      produceDynamicWorkUnitUpdateEvent();
    } else {
      selfKillContainer();
    }
  }

  private void produceDynamicWorkUnitUpdateEvent() {
    if (this.eventBus != null) {
      log.info("Posting {} message to EventBus", WorkUnitLaggingEvent.class.getSimpleName());
      WorkUnitLaggingEvent event = new WorkUnitLaggingEvent(this.config, getClass().getName());
      event.addMetadata(WorkUnitLaggingEvent.WORK_UNIT_LAGGING_TOPIC_PARTITION_KEY,
          healthModel.getLaggingTopicPartitions().toString());
      addIngestionMetrics(event);
      this.eventBus.post(event);
    }
  }

  /**
   * Post container health check failure event. This event will be consumed by GobblinTaskRunner class to kill the
   * container and emit GTE
   */
  private void selfKillContainer() {
    if (this.eventBus != null) {
      log.info("Posting {} message to EventBus", ContainerHealthCheckFailureEvent.class.getSimpleName());
      ContainerHealthCheckFailureEvent event = new ContainerHealthCheckFailureEvent(this.config, getClass().getName());
      addIngestionMetrics(event);
      this.eventBus.post(event);
    }
  }

  private void addIngestionMetrics(MetadataBasedEvent event) {
    event.addMetadata("ingestionLatencies", healthModel.getIngestionLatencies().toString());
    event.addMetadata("consumptionRates", healthModel.getConsumptionRateMBps().toString());
    event.addMetadata("ingestionLatencyThreshold", Long.toString(healthModel.getIngestionLatencyThresholdMinutes()));
    event.addMetadata("targetConsumptionRate", Double.toString(healthModel.getExpectedConsumptionRate()));
  }

  private EventBus getEventBus(String name) {
    EventBus eventBus;
    try {
      eventBus = EventBusFactory.get(ContainerHealthCheckFailureEvent.CONTAINER_HEALTH_CHECK_EVENT_BUS_NAME,
          SharedResourcesBrokerFactory.getImplicitBroker());
    } catch (IOException e) {
      log.error("Could not find EventBus instance for container health check", e);
      eventBus = null;
    }
    return eventBus;
  }
}
