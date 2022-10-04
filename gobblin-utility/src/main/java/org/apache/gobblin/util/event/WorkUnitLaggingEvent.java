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

/**
 * An event type to signal failure of a container health check. This event can be generated from anywhere
 * inside the application. This event is intended to be emitted
 * over an {@link com.google.common.eventbus.EventBus} instance.
 */
package org.apache.gobblin.util.event;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;


@ToString
public class WorkUnitLaggingEvent implements MetadataBasedEvent {
  public static final String WORK_UNIT_LAGGING_EVENT_BUS_NAME = "WorkUnitLaggingEventBus";
  public static final String WORK_UNIT_LAGGING_TOPIC_PARTITION_KEY = "LaggingTopicPartitions";

  // Context of emission of this event, like the task's state.
  @Getter
  private final Config config;

  /**
   * Name of the class that generated this failure event.
   */
  @Getter
  private final String className;

  @Getter
  private final Map<String, String> metadata = Maps.newHashMap();

  public WorkUnitLaggingEvent(Config config, String className) {
    this.config = config;
    this.className = className;
  }

  public List<String> getLaggingTopicPartitions() {
    String laggingTopicPartitions = metadata.get(WORK_UNIT_LAGGING_TOPIC_PARTITION_KEY);
    return asList(laggingTopicPartitions);
  }

  @Override
  public String getName() {
    return WorkUnitLaggingEvent.class.getSimpleName();
  }

  public void addMetadata(String key, String value) {
    metadata.put(key, value);
  }


  private static final String LIST_DELIMITTER = ",";
  private static final Splitter LIST_SPLITTER = Splitter.on(LIST_DELIMITTER).trimResults().omitEmptyStrings();

  /**
   * Converts {@link List<String>#toString()} representation of a {@link List<String>} back into a {@link List<String>}.
   * This method should not be used with values that contain commas.
   * @param value toString representation of a {@link List<String>}
   * @return Best effort representation of the original list
   */
  @VisibleForTesting
  static List<String> asList(String value) {
    if (StringUtils.isEmpty(value)) {
      return Collections.emptyList();
    }
    return LIST_SPLITTER.splitToList(value.substring(1, value.length() - 1));
  }

}
