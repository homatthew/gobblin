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

import java.util.List;
import org.apache.gobblin.runtime.messaging.MessageBuffer;
import org.apache.gobblin.runtime.messaging.data.DynamicWorkUnitMessage;
import org.apache.hadoop.fs.FileSystem;


/**
 * Implements {@link FileSystem} based message buffer for sending and receiving {@link DynamicWorkUnitMessage}.
 */
public class FileSystemMessageBuffer implements MessageBuffer<DynamicWorkUnitMessage> {

  @Override
  public boolean add(DynamicWorkUnitMessage item) {
    // STUB: TODO GOBBLIN-1685
    return false;
  }

  @Override
  public List<DynamicWorkUnitMessage> get() {
    // STUB: TODO GOBBLIN-1685
    return null;
  }
}
