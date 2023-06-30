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

// @@@SNIPSTART hello-world-project-template-java-worker
package org.apache.gobblin.cluster.temporal;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

public class GobblinTemporalWorker {

    public static void main(String[] args) {

        // Get a Workflow service stub.
        WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();

        /*
        * Get a Workflow service client which can be used to start, Signal, and Query Workflow Executions.
        */
        WorkflowClient client = WorkflowClient.newInstance(service);

        /*
        * Define the workflow factory. It is used to create workflow workers that poll specific Task Queues.
        */
        WorkerFactory factory = WorkerFactory.newInstance(client);

        /*
        * Define the workflow worker. Workflow workers listen to a defined task queue and process
        * workflows and activities.
        */
        Worker worker = factory.newWorker(Shared.HELLO_WORLD_TASK_QUEUE);

        /*
        * Register our workflow implementation with the worker.
        * Workflow implementations must be known to the worker at runtime in
        * order to dispatch workflow tasks.
        */
        worker.registerWorkflowImplementationTypes(GobblinTemporalWorkflowImpl.class);

        /*
        * Register our Activity Types with the Worker. Since Activities are stateless and thread-safe,
        * the Activity Type is a shared instance.
        */
        worker.registerActivitiesImplementations(new GobblinTemporalActivitiesImpl());

        /*
        * Start all the workers registered for a specific task queue.
        * The started workers then start polling for workflows and activities.
        */
        factory.start();

    }
}
// @@@SNIPEND
