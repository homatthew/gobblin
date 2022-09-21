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

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class KafkaExtractorStats {
  private KafkaExtractorStats() { }

  private enum Metrics {
    MS_PER_RECORD,
    PARTITION_LATENCY,
    DECODING_ERROR
  }

  private static class KafkaExtractorStatsHistogram {

    Reservoir reservoir;
    Map<Metrics , Histogram> stats;
    public KafkaExtractorStatsHistogram(Reservoir reservoir) {
      this.reservoir = reservoir;
      this.stats = new HashMap<>();
      for (Metrics m : Metrics.values()) {
        this.stats.put(m, new Histogram(reservoir));
      }
    }

    public void init(Collection<KafkaExtractorStatsTracker.ExtractorStats> allStats) {
      for (KafkaExtractorStatsTracker.ExtractorStats stats : allStats) {
        update(Metrics.MS_PER_RECORD, Double.valueOf(stats.getAvgMillisPerRecord()).longValue());
        update(Metrics.DECODING_ERROR, Double.valueOf(stats.getDecodingErrorCount()).longValue());

        long partitionLatency = calcPartitionLatency(stats);
        if (partitionLatency > 0) {
          update(Metrics.PARTITION_LATENCY, partitionLatency);
        }
      }
    }

    public void update(Metrics metrics, long value) {
      this.stats.get(metrics).update(value);
    }

    public Snapshot getSnapshot(Metrics metrics) {
      return stats.get(metrics).getSnapshot();
    }

  }

  public static Set<KafkaPartition> getTopHitters(
      Map<KafkaPartition, KafkaExtractorStatsTracker.ExtractorStats> statsMap) {
    Set<KafkaPartition> topHitters = new HashSet<>();
    ExponentiallyDecayingReservoir reservoir = new ExponentiallyDecayingReservoir();
    KafkaExtractorStatsHistogram histogram = new KafkaExtractorStatsHistogram(reservoir);
    histogram.init(statsMap.values());

    statsMap.forEach((partition, statsTracker) -> {
      if (isHeavyHitter(statsTracker, histogram)) {
        topHitters.add(partition);
      }
    });

    return topHitters;
  }

  static boolean isHeavyHitter(KafkaExtractorStatsTracker.ExtractorStats extractorStats, KafkaExtractorStatsHistogram histogram) {
    if (calcPartitionLatency(extractorStats) >= histogram.getSnapshot(Metrics.PARTITION_LATENCY).get75thPercentile()) {
      return true;
    }

    if (extractorStats.getDecodingErrorCount() > 0) {
      if (extractorStats.getDecodingErrorCount() >= histogram.getSnapshot(Metrics.DECODING_ERROR).get95thPercentile()) {
        return true;
      }
    }

    if (extractorStats.getAvgMillisPerRecord() >= histogram.getSnapshot(Metrics.DECODING_ERROR).get95thPercentile()) {
      return true;
    }

    return false;
  }

  static long calcPartitionLatency(KafkaExtractorStatsTracker.ExtractorStats partitionStats) {
    long partitionLatency = -1;
    //Check if there are any records consumed from this KafkaPartition.
    if (partitionStats.getMinLogAppendTime() > 0) {
      partitionLatency = partitionStats.getStopFetchEpochTime() - partitionStats.getMinLogAppendTime();
    }
    return partitionLatency;
  }
}
