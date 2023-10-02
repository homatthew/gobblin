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

package org.apache.gobblin.temporal.workflows.timing;

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.slf4j.Logger;

import io.temporal.workflow.Workflow;

import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.temporal.workflows.trackingevent.activity.GobblinTrackingEventActivity;


/**
 * Boiler plate for tracking elapsed time of events in a {@link Workflow} or {@link io.temporal.activity.Activity}
 */
public class GobblinTemporalTimer implements Closeable {
  private static final Logger log = Workflow.getLogger(GobblinTemporalTimer.class);
  private final GobblinTrackingEventActivity trackingEventActivity;
  private final List<Tag<?>> tags;
  private final GobblinEventBuilder eventBuilder;

  private Instant startTime;

  private GobblinTemporalTimer(GobblinTrackingEventActivity trackingEventActivity, List<Tag<?>> tags, GobblinEventBuilder eventBuilder) {
    this.trackingEventActivity = trackingEventActivity;
    this.tags = tags;
    this.eventBuilder = eventBuilder;
    this.startTime = getCurrentTime();
    log.info("mho: Starting a timer at eventName={}, startTime={}", eventBuilder.getName(), this.startTime);
  }

  @Override
  public void close() {
    Instant endTime = getCurrentTime();
    this.eventBuilder.addMetadata(EventSubmitter.EVENT_TYPE, TimingEvent.METADATA_TIMING_EVENT);
    this.eventBuilder.addMetadata(TimingEvent.METADATA_START_TIME, Long.toString(this.startTime.toEpochMilli()));
    this.eventBuilder.addMetadata(TimingEvent.METADATA_END_TIME, Long.toString(endTime.toEpochMilli()));
    Duration duration = Duration.between(this.startTime, endTime);
    this.eventBuilder.addMetadata(TimingEvent.METADATA_DURATION, Long.toString(duration.toMillis()));

    trackingEventActivity.submitGTE(this.eventBuilder, this.tags);
    log.info("mho: Ending a timer at eventName={}, endTime={}", eventBuilder.getName(), endTime);
  }

  private Instant getCurrentTime() {
    return Instant.ofEpochMilli(Workflow.currentTimeMillis());
  }

  public static class Factory {
    private final GobblinTrackingEventActivity trackingEventActivity;
    private final List<Tag<?>> tags;
    private final String namespace;

    public Factory(GobblinTrackingEventActivity trackingEventActivity, String namespace, List<Tag<?>> tags) {
      this.trackingEventActivity = trackingEventActivity;
      this.namespace = namespace;
      this.tags = tags;
    }

    public GobblinTemporalTimer get(String eventName) {
      GobblinEventBuilder eventBuilder = new GobblinEventBuilder(eventName, namespace);
      return new GobblinTemporalTimer(this.trackingEventActivity, this.tags, eventBuilder);
    }

    /**
     * Some funny boiler plate for emitting a start event at the beginning and ending of a try statement
     *
     * The endtimer has the same start time as the first submitted start time but that value might be ignored
     * @return
     */
    public Closeable getWorkflowTimer() {
      GobblinTemporalTimer startTimer = get(TimingEvent.LauncherTimings.JOB_START);
      Instant startTime = startTimer.startTime;
      startTimer.close();

      return () -> {
        GobblinTemporalTimer endTimer = get(TimingEvent.LauncherTimings.JOB_COMPLETE);
        endTimer.startTime = startTime;
        endTimer.close();
      };
    }
  }
}
