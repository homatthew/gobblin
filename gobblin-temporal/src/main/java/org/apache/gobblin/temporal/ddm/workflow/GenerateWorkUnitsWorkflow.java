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

package org.apache.gobblin.temporal.ddm.workflow;

import java.util.Properties;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.temporal.workflows.metrics.EventSubmitterContext;


/** Workflow simply to generate {@link WorkUnit}s from a {@link org.apache.gobblin.source.Source} (and persist them for subsequent processing) */
@WorkflowInterface
public interface GenerateWorkUnitsWorkflow {
  /** @return the number of {@link WorkUnit}s generated and persisted */
  @WorkflowMethod
  int generate(Properties props, EventSubmitterContext eventSubmitterContext);
}
