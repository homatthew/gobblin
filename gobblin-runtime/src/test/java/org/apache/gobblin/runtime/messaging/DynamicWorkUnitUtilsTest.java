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
package org.apache.gobblin.runtime.messaging;

import com.google.gson.JsonElement;
import java.util.Arrays;
import org.apache.gobblin.runtime.messaging.data.DynamicWorkUnitMessage;
import org.apache.gobblin.runtime.messaging.data.SplitWorkUnitMessage;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;


@Test
public class DynamicWorkUnitUtilsTest {
  @Test
  public void testSerialization() {
    DynamicWorkUnitMessage msg = SplitWorkUnitMessage.builder()
        .workUnitId("workunitId")
        .laggingTopicPartitions(Arrays.asList("topic-1","topic-2"))
        .build();

    JsonElement serializedMsg = msg.toJson();

    DynamicWorkUnitMessage deserializedMsg = DynamicWorkUnitUtils.fromJson(serializedMsg);

    assertTrue(deserializedMsg instanceof SplitWorkUnitMessage);
    assertEquals(msg, deserializedMsg);
  }
}
