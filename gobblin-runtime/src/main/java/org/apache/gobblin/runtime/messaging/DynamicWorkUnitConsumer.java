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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.gobblin.runtime.messaging.data.DynamicWorkUnitMessage;


/**
 * Abstraction for receiving {@link DynamicWorkUnitMessage} from {@link DynamicWorkUnitProducer}.
 * The class is responsible for fetching the messages from the messaging service. All business logic
 * should be done in the {@link DynamicWorkUnitMessage.Handler}.<br><br>
 *
 * For polling implementations (e.g. HDFS or Kafka), you can use the
 * {@link DynamicWorkUnitUtils#runInBackground(Runnable, Duration)} to call the
 * {@link Runnable#run()} method periodically in a background thread <br><br>
 *
 * Push based implementations (e.g. helix or zk) can omit using this method and instead setup the callback methods
 * without spawning a background thread
 */
public abstract class DynamicWorkUnitConsumer implements Runnable {
  protected List<DynamicWorkUnitMessage.Handler> handlers = new ArrayList<>();

  protected void handleMessage(DynamicWorkUnitMessage msg) {
    for (DynamicWorkUnitMessage.Handler handler : handlers) {
      handler.handle(msg);
    }
  }

  protected void addHandler(DynamicWorkUnitMessage.Handler handler) {
    handlers.add(handler);
  }
}
