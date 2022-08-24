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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.gobblin.runtime.messaging.data.DynamicWorkUnitMessage;


/**
 * Abstraction for receiving {@link DynamicWorkUnitMessage} from {@link DynamicWorkUnitProducer}.
 * The class is responsible for fetching the messages from the messaging service. All business logic
 * should be done in the {@link DynamicWorkUnitMessage.Handler}.<br><br>
 *
 * This consumers can be used to poll a message buffer (e.g. HDFS or Kafka) using
 * {@link ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)} to call the
 * {@link Runnable#run()} method periodically in a background thread <br><br>
 *
 * Each new {@link DynamicWorkUnitMessage} is passed to a {@link DynamicWorkUnitMessage.Handler}
 * and will call {@link DynamicWorkUnitMessage.Handler#handle(DynamicWorkUnitMessage)}
 */
public class DynamicWorkUnitConsumer implements Runnable {
  protected MessageBuffer<DynamicWorkUnitMessage> buffer;
  protected List<DynamicWorkUnitMessage.Handler> handlers;

  public DynamicWorkUnitConsumer(
      MessageBuffer<DynamicWorkUnitMessage> buffer,
      Collection<DynamicWorkUnitMessage.Handler> handlers) {
    this.buffer = buffer;
    for(DynamicWorkUnitMessage.Handler handler : handlers) {
      handlers.add(handler);
    }
  }

  /**
   * Fetches all unread messages from sent by {@link DynamicWorkUnitProducer} and
   * calls {@link DynamicWorkUnitMessage.Handler#handle(DynamicWorkUnitMessage)} method for each handler added via
   * {@link DynamicWorkUnitConsumer#DynamicWorkUnitConsumer(MessageBuffer, Collection)} or
   * {@link DynamicWorkUnitConsumer#addHandler(DynamicWorkUnitMessage.Handler)}
   */
  public void run() {
    List<DynamicWorkUnitMessage> messages = buffer.get();
    for (DynamicWorkUnitMessage msg : messages) {
      handleMessage(msg);
    }
  }

  protected void handleMessage(DynamicWorkUnitMessage msg) {
    for (DynamicWorkUnitMessage.Handler handler : handlers) {
      handler.handle(msg);
    }
  }

  protected void addHandler(DynamicWorkUnitMessage.Handler handler) {
    handlers.add(handler);
  }
}
