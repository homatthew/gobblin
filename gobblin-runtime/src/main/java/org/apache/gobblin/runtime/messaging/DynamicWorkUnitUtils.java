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

import com.google.common.base.Optional;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import lombok.NonNull;
import org.apache.gobblin.runtime.messaging.data.DynamicWorkUnitMessage;
import org.apache.gobblin.util.LoggingUncaughtExceptionHandler;


public final class DynamicWorkUnitUtils {
  private static final Gson GSON = new Gson();
  private static final String PROPS_PREFIX = "DynamicWorkUnit.Props";
  private static final String MESSAGE_IMPLEMENTATION = PROPS_PREFIX + ".MessageImplementationClass";

  private DynamicWorkUnitUtils() {
    throw new RuntimeException("Cannot instantiate utils class");
  }

  /**
   * Helper method for deserializing {@link DynamicWorkUnitMessage}
   * @param json Serialized message using {@link DynamicWorkUnitUtils#toJson}
   * @return {@link DynamicWorkUnitMessage} POJO representation of the given json
   */
  public static <T extends DynamicWorkUnitMessage> DynamicWorkUnitMessage fromJson(JsonElement json) {
    try {
      JsonObject obj = json.getAsJsonObject();
      Class<T> clazz = (Class<T>) Class.forName(obj.get(MESSAGE_IMPLEMENTATION).getAsString());
      return GSON.fromJson(json, clazz);
    } catch (IllegalStateException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Helper method for serializing {@link DynamicWorkUnitMessage}
   * @param msg Message object to serialize
   * @return json representation of message object
   */
  public static JsonElement toJson(@NonNull DynamicWorkUnitMessage msg) {
    JsonElement json = GSON.toJsonTree(msg);
    JsonObject obj = json.getAsJsonObject();
    obj.addProperty(MESSAGE_IMPLEMENTATION, msg.getClass().getName());
    return obj;
  }

  /**
   * Helper method for periodically executing runnable. Useful method to allow {@link DynamicWorkUnitConsumer} to poll
   * for new messages
   * @param runnable
   * @param timeBetweenRunStarts
   */
  public static void runInBackground(Runnable runnable, Duration timeBetweenRunStarts) {
    Thread idleProcessThread = new Thread(() -> {
      while (true) {
        Instant nextStartTime = Instant.now().plus(timeBetweenRunStarts);
        runnable.run();
        try {
          long timeToSleepMs = Instant.now().until(nextStartTime, ChronoUnit.MILLIS);
          if(timeToSleepMs > 0) {
            Thread.sleep(timeToSleepMs);
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });

    //Log uncaught exceptions (e.g.OOMEs) to prevent threads from dying silently
    idleProcessThread.setName(runnable.getClass().getSimpleName());
    idleProcessThread.setUncaughtExceptionHandler(new LoggingUncaughtExceptionHandler(Optional.absent()));
    idleProcessThread.start();
  }
}
