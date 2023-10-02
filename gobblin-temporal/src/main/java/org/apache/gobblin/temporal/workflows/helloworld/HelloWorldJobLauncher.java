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

package org.apache.gobblin.temporal.workflows.helloworld;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

import io.temporal.client.WorkflowOptions;
import io.temporal.workflow.Workflow;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.temporal.cluster.GobblinTemporalTaskRunner;
import org.apache.gobblin.temporal.joblauncher.GobblinTemporalJobLauncher;
import org.apache.gobblin.temporal.joblauncher.GobblinTemporalJobScheduler;


/**
 * An implementation of {@link JobLauncher} that launches a Gobblin job using the Temporal task framework.
 *
 * <p>
 *   This class is instantiated by the {@link GobblinTemporalJobScheduler#buildJobLauncher(Properties)} on every job submission to launch the Gobblin job.
 *   The actual task execution happens in the {@link GobblinTemporalTaskRunner}, usually in a different process.
 * </p>
 */
@Alpha
public class HelloWorldJobLauncher extends GobblinTemporalJobLauncher {
  private static Logger log = Workflow.getLogger(HelloWorldJobLauncher.class);

  public HelloWorldJobLauncher(Properties jobProps, Path appWorkDir, List<? extends Tag<?>> metadataTags,
      ConcurrentHashMap<String, Boolean> runningMap)
      throws Exception {
    super(jobProps, appWorkDir, metadataTags, runningMap);
  }

  @Override
  public void submitJob(List<WorkUnit> workunits) {
    WorkflowOptions options = WorkflowOptions.newBuilder().setTaskQueue(queueName).build();
    GreetingWorkflow greetingWorkflow = this.client.newWorkflowStub(GreetingWorkflow.class, options);

    List<Tag<?>> tags = getTags(jobProps, eventSubmitter);
    String greeting = greetingWorkflow.getGreeting("Gobblin", eventSubmitter.getNamespace(), tags);
    log.info(greeting);
  }

  public static List<Tag<?>> getTags(Properties jobProps, EventSubmitter eventSubmitter) {
    Map<String, Tag<?>> tagsMap = new HashMap<>();
    // this step might be unnecessary because there metrics classes that call this method already and fill in the job props
    // at this point
    GobblinMetrics.getCustomTagsFromState(new State(jobProps)).forEach(tag -> tagsMap.put(tag.getKey(), tag));
    eventSubmitter.getTags().forEach(tag -> {
      if (tagsMap.containsKey(tag.getKey())) {
        log.warn("Duplicate tag key: " + tag.getKey());
      }

      tagsMap.put(tag.getKey(), tag);
    });

    return new ArrayList<>(tagsMap.values());
  }
}
