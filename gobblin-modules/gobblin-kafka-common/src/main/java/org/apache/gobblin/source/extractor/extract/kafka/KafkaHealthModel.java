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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.EvictingQueue;
import com.typesafe.config.Config;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.source.extractor.extract.kafka.workunit.packer.KafkaTopicGroupingWorkUnitPacker;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.event.ContainerHealthCheckFailureEvent;


/**
 * Class for modeling the current kafka ingestion health. This class is responsible for the maintaining the data and api's
 * for checking on kafka ingestion health. This class is different from the {@link KafkaExtractorStatsTracker} because it
 * contains the logic / definitions for whether a metric is considered "healthy"
 */
@Slf4j
@Alias(value = "KafkaHealthModel")
@Getter
public class KafkaHealthModel {
  public static final String KAFKA_INGESTION_HEALTH_PREFIX = "gobblin.kafka.healthCheck.";
  public static final String KAFKA_INGESTION_HEALTH_SLIDING_WINDOW_SIZE_KEY = KAFKA_INGESTION_HEALTH_PREFIX + "slidingWindow.size";
  public static final String KAFKA_INGESTION_HEALTH_LATENCY_THRESHOLD_MINUTES_KEY = KAFKA_INGESTION_HEALTH_PREFIX + "ingestionLatency.minutes";
  public static final String KAFKA_INGESTION_HEALTH_CONSUMPTION_RATE_DROPOFF_FRACTION_KEY = KAFKA_INGESTION_HEALTH_PREFIX + "consumptionRate.dropOffFraction";
  public static final String KAFKA_INGESTION_HEALTH_INCREASING_LATENCY_CHECK_ENABLED_KEY = KAFKA_INGESTION_HEALTH_PREFIX + "increasingLatencyCheckEnabled";

  private static final int DEFAULT_KAFKA_INGESTION_HEALTH_MODEL_SLIDING_WINDOW_SIZE = 3;
  private static final long DEFAULT_KAFKA_INGESTION_HEALTH_MODEL_LATENCY_THRESHOLD_MINUTES = 15;
  private static final double DEFAULT_KAFKA_INGESTION_HEALTH_MODEL_CONSUMPTION_RATE_DROPOFF_FRACTION = 0.7;
  private static final boolean DEFAULT_KAFKA_INGESTION_HEALTH_MODEL_INCREASING_LATENCY_CHECK_ENABLED = true;

  private final Config config;
  private final KafkaExtractorStatsTracker statsTracker;
  private final double expectedConsumptionRate;
  private final double consumptionRateDropOffFraction;
  private final long ingestionLatencyThresholdMinutes;
  private final int slidingWindowSize;
  private final EvictingQueue<Long> ingestionLatencies;
  private final EvictingQueue<Double> consumptionRateMBps;
  private final boolean increasingLatencyCheckEnabled;
  private final Cache<KafkaPartition, Void> laggingTopicPartitions;

  public KafkaHealthModel(Config config, KafkaExtractorStatsTracker statsTracker) {
    this.config = config;
    this.slidingWindowSize = ConfigUtils.getInt(config, KAFKA_INGESTION_HEALTH_SLIDING_WINDOW_SIZE_KEY,
        DEFAULT_KAFKA_INGESTION_HEALTH_MODEL_SLIDING_WINDOW_SIZE);
    this.ingestionLatencyThresholdMinutes = ConfigUtils.getLong(config,
        KAFKA_INGESTION_HEALTH_LATENCY_THRESHOLD_MINUTES_KEY,
        DEFAULT_KAFKA_INGESTION_HEALTH_MODEL_LATENCY_THRESHOLD_MINUTES);
    this.consumptionRateDropOffFraction = ConfigUtils.getDouble(config,
        KAFKA_INGESTION_HEALTH_CONSUMPTION_RATE_DROPOFF_FRACTION_KEY,
        DEFAULT_KAFKA_INGESTION_HEALTH_MODEL_CONSUMPTION_RATE_DROPOFF_FRACTION);
    this.expectedConsumptionRate = ConfigUtils.getDouble(config, KafkaTopicGroupingWorkUnitPacker.CONTAINER_CAPACITY_KEY, KafkaTopicGroupingWorkUnitPacker.DEFAULT_CONTAINER_CAPACITY);
    this.increasingLatencyCheckEnabled = ConfigUtils.getBoolean(config,
        KAFKA_INGESTION_HEALTH_INCREASING_LATENCY_CHECK_ENABLED_KEY,
        DEFAULT_KAFKA_INGESTION_HEALTH_MODEL_INCREASING_LATENCY_CHECK_ENABLED);
    this.ingestionLatencies = EvictingQueue.create(this.slidingWindowSize);
    this.consumptionRateMBps = EvictingQueue.create(this.slidingWindowSize);
    this.laggingTopicPartitions = CacheBuilder.newBuilder()
        .expireAfterWrite(this.slidingWindowSize, TimeUnit.MINUTES)
        .build();
    this.statsTracker = statsTracker;
  }

  public boolean hasEnoughData() {
    return getNumberOfObservations() == this.slidingWindowSize;
  }

  public int getNumberOfObservations() {
    return ingestionLatencies.size();
  }

  /**
   *
   * @return false if (i) ingestionLatency in the each of the recent epochs exceeds the threshold latency , AND (ii)
   * if {@link KafkaHealthModel#increasingLatencyCheckEnabled} is true, the latency
   * is increasing over these epochs.
   */
  public boolean isIngestionLatencyHealthy() {
    Long previousLatency = -1L;
    for (Long ingestionLatency: ingestionLatencies) {
      if (ingestionLatency < this.ingestionLatencyThresholdMinutes) {
        return true;
      } else {
        if (this.increasingLatencyCheckEnabled) {
          if (previousLatency > ingestionLatency) {
            return true;
          }
          previousLatency = ingestionLatency;
        }
      }
    }
    return false;
  }

  public boolean isConsumptionRateHealthy() {
    return getMaxConsumptionRate() > this.consumptionRateDropOffFraction * this.expectedConsumptionRate;
  }

  /**
   * @return Return a serialized string representation of health check report.
   */
  public String getHealthCheckReport() {
    return String.format("Ingestion Latencies = %s, Ingestion Latency Threshold = %s minutes, "
        + "Consumption Rates = %s, Target Consumption Rate = %s MBps", this.ingestionLatencies.toString(),
        this.ingestionLatencyThresholdMinutes, this.consumptionRateMBps.toString(), this.expectedConsumptionRate);
  }

  /**
   * Execute the commit step. The execute method gets the maximum ingestion latency and the consumption rate and emits
   * a {@link ContainerHealthCheckFailureEvent} if the following conditions are satisfied:
   * <li>
   *   <ul>The ingestion latency increases monotonically over the {@link KafkaHealthModel#slidingWindowSize} intervals, AND </ul>
   *   <ul>The maximum consumption rate over the {@link KafkaHealthModel#slidingWindowSize} intervals is smaller than
   *   {@link KafkaHealthModel#consumptionRateDropOffFraction} * {@link KafkaHealthModel#expectedConsumptionRate}</ul>.
   * </li>
   *
   * The {@link ContainerHealthCheckFailureEvent} is posted to a global event bus. The handlers of this event type
   * can perform suitable actions based on the execution environment.
   */
  public void update() {
    this.ingestionLatencies.add(this.statsTracker.getMaxIngestionLatency(TimeUnit.MINUTES));
    this.consumptionRateMBps.add(this.statsTracker.getConsumptionRateMBps());
  }

  public double getMaxConsumptionRate() {
    return consumptionRateMBps.stream().mapToDouble(d->d)
        .filter(consumptionRate -> consumptionRate >= 0.0)
        .max()
        .orElse(0.0);
  }

  public Set<String> getLaggingTopicPartitions() {
    return statsTracker.getTopHitters().stream().map(KafkaPartition::toString)
        .collect(Collectors.toSet());
  }
}
