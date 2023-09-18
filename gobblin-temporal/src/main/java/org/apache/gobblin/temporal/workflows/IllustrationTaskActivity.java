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

package org.apache.gobblin.temporal.workflows;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
/**
 * Activity for processing {@link IllustrationTask}s
 *
 * CAUTION/FINDING: an `@ActivityInterface` must not be parameterized (e.g. here, by TASK), as doing so results in:
 *   io.temporal.failure.ApplicationFailure: message='class java.util.LinkedHashMap cannot be cast to class
 *       com.linkedin.temporal.app.work.IllustrationTask', type='java.lang.ClassCastException'
 */
@ActivityInterface
public interface IllustrationTaskActivity {
    @ActivityMethod
    String doTask(IllustrationTask task);
}
