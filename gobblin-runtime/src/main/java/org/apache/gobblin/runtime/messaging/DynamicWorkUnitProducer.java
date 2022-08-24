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

import lombok.AllArgsConstructor;
import org.apache.gobblin.runtime.messaging.data.DynamicWorkUnitMessage;


/**
 * Abstraction for sending {@link DynamicWorkUnitMessage} that will be consumed by a {@link DynamicWorkUnitConsumer}
 * <br><br>
 *
 * A {@link DynamicWorkUnitProducer} has a tight coupling with the {@link DynamicWorkUnitConsumer}
 * since both producer / consumer should be using the same {@link MessageBuffer} (i.e. via Helix, hdfs, Kafka, etc).
 */
@AllArgsConstructor
public class DynamicWorkUnitProducer {
  protected MessageBuffer<DynamicWorkUnitMessage> buffer;

  /**
   * Produce a message to be consumed by a {@link DynamicWorkUnitMessage}
   * @param message Message to be sent over the message buffer
   */
  public void produce(DynamicWorkUnitMessage message) {
    buffer.add(message);
  }
}
