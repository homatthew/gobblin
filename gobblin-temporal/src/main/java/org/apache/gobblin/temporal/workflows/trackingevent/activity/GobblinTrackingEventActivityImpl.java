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

package org.apache.gobblin.temporal.workflows.trackingevent.activity;

import java.util.List;

import org.slf4j.Logger;

import io.temporal.workflow.Workflow;

import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;


public class GobblinTrackingEventActivityImpl implements GobblinTrackingEventActivity {
    private static Logger log = Workflow.getLogger(GobblinTrackingEventActivityImpl.class);

    @Override
    public void submitGTE(GobblinEventBuilder eventBuilder, List<Tag<?>> tags) {
        log.info("mho: Submitting an event");
        getEventSubmitter(tags).submit(eventBuilder);
        log.info("mho: Finished submitting an event");
    }

    private EventSubmitter getEventSubmitter(List<Tag<?>> tags) {
        MetricContext metricContext = MetricContext.builder("<GobblinTrackingEventActivityImpl METRIC CONTEXT NAME>").addTags(tags).build();
        return new EventSubmitter.Builder(metricContext, "gobblin.runtime").build();
    }
}
