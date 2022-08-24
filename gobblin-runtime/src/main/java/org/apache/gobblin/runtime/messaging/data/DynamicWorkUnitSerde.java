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
package org.apache.gobblin.runtime.messaging.data;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.nio.charset.Charset;
import lombok.NonNull;


public final class DynamicWorkUnitSerde {
  private static final Gson GSON = new Gson();
  private static final String PROPS_PREFIX = "DynamicWorkUnit.Props";
  private static final String MESSAGE_IMPLEMENTATION = PROPS_PREFIX + ".MessageImplementationClass";
  private static final Charset DEFAULT_CHAR_ENCODING = Charsets.UTF_8;

  private DynamicWorkUnitSerde() {
    throw new RuntimeException("Cannot instantiate utils class");
  }

  /**
   * Serialize message into bytes. Deserialization should happen using {@link DynamicWorkUnitSerde#deserialize(byte[])}
   * Serialization preserves underlying properties of the implementating {@link DynamicWorkUnitMessage}.<br><br>
   * For example, the {@link SplitWorkUnitMessage} implements
   * {@link DynamicWorkUnitMessage} and has implementation specific properties such as
   * {@link SplitWorkUnitMessage#getLaggingTopicPartitions()}. These properties will be maintained after serde.
   * @param msg message to serialize
   * @return message to bytes
   */
  public static byte[] serialize(DynamicWorkUnitMessage msg) {
    Preconditions.checkNotNull(msg, "Input message cannot be null");
    return toJsonObject(msg)
        .toString()
        .getBytes(DEFAULT_CHAR_ENCODING);
  }

  /**
   * Deserialize bytes into message object. Serialization should have been done using
   * {@link DynamicWorkUnitSerde#deserialize(byte[])}.
   * @param serializedMessage message that has been serialized by {@link DynamicWorkUnitSerde#serialize(DynamicWorkUnitMessage)}
   * @return DynamicWorkUnitMessage object
   */
  public static DynamicWorkUnitMessage deserialize(byte[] serializedMessage) {
    String json = new String(serializedMessage, DEFAULT_CHAR_ENCODING);
    JsonObject jsonObject = GSON.fromJson(json, JsonObject.class);
    return toDynamicWorkUnitMessage(jsonObject);
  }

  /**
   * Helper method for deserializing {@link JsonObject} to {@link DynamicWorkUnitMessage}
   * @param json Message serialized using {@link DynamicWorkUnitSerde#toJsonObject}
   * @return {@link DynamicWorkUnitMessage} POJO representation of the given json
   */
  private static <T extends DynamicWorkUnitMessage> DynamicWorkUnitMessage toDynamicWorkUnitMessage(JsonObject json) {
    Preconditions.checkNotNull(json, "Serialized msg cannot be null");
    try {
      if (!json.has(MESSAGE_IMPLEMENTATION)) {
        throw new DynamicWorkUnitDeserializationException(
            String.format("Unable to deserialize json to %s. Ensure that %s "
                    + "is used for serialization. %s does not have the key=%s used for deserializing to correct message "
                    + "implementation. json=%s",
                DynamicWorkUnitMessage.class.getSimpleName(),
                "DynamicWorkSerde#serialize(DynamicWorkUnitMessage msg)",
                json.getClass().getSimpleName(),
                MESSAGE_IMPLEMENTATION,
                json));
      }
      Class<T> clazz = (Class<T>) Class.forName(json.get(MESSAGE_IMPLEMENTATION).getAsString());
      return GSON.fromJson(json, clazz);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(
          String.format("Input param %s contains invalid value for key=%s. This can be caused by the deserializer having"
                  + " different dependencies from the serializer. json=%s",
              json.getClass(),
              MESSAGE_IMPLEMENTATION,
              json), e);
    }
  }

  /**
   * Helper method for serializing {@link DynamicWorkUnitMessage}
   * @param msg Message object to serialize
   * @return json representation of message object
   */
  private static JsonObject toJsonObject(@NonNull DynamicWorkUnitMessage msg) {
    Preconditions.checkNotNull(msg, "Input message cannot be null");
    JsonElement json = GSON.toJsonTree(msg);
    JsonObject obj = json.getAsJsonObject();
    obj.addProperty(MESSAGE_IMPLEMENTATION, msg.getClass().getName());
    return obj;
  }
}
