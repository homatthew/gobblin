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
package org.apache.gobblin.runtime.messaging.hdfs;

import com.google.gson.JsonElement;
import java.io.IOException;
import java.util.Collection;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * Implements {@link FileSystem} based message buffer for sending {@link JsonElement} based messages. This buffer does
 * not guarantee FIFO and does not have any safeguards for concurrent readers / writers.
 */
@AllArgsConstructor
public class FileSystemMessageBuffer {
  private final FileSystem fs;
  private final Path dir;

  /**
   * Write a {@link JsonElement} to the buffer
   * @param message message to be written
   * @return if the message was successfully persisted to {@link FileSystem}
   */
  public boolean add(JsonElement message) {
    // STUB: TODO GOBBLIN-1685
    return true;
  }

  /**
   * Get all messages from the buffer that have not been {@link AcknowledgeableMessage#ack()}
   * @return list of messages that still need to be processed
   */
  public Collection<AcknowledgeableMessage> getUnacknowledgedMessages() {
    // STUB: TODO GOBBLIN-1685
    return null;
  }

  private boolean delete(Path filePath) throws IOException {
    // STUB: TODO GOBBLIN-1685
    return true;
  }

  /**
   * Wrapper for {@link JsonElement} messages on disk that provides api's for an acknowledgement after processing
   * (i.e. cleaning up the file on disk after done processing)
   */
  @AllArgsConstructor
  public static class AcknowledgeableMessage {
    @Getter
    private JsonElement message;
    private Path messagePath;
    private FileSystemMessageBuffer messageBuffer;

    /**
     * Clean up underlying message state. Meant to be called after the message has been processed.
     * @return message is successfully deleted
     */
    public boolean ack() {
      try {
        return messageBuffer.delete(messagePath);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
