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
package gobblin.data.management.conversion.hive.writer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import lombok.AllArgsConstructor;

import gobblin.configuration.State;
import gobblin.data.management.conversion.hive.entities.QueryBasedHiveConversionEntity;
import gobblin.data.management.conversion.hive.events.EventWorkunitUtils;
import gobblin.util.HiveJdbcConnector;
import gobblin.writer.DataWriter;
import lombok.extern.slf4j.Slf4j;


/**
 * The {@link HiveQueryExecutionWriter} is responsible for running the hive query available at
 * {@link QueryBasedHiveConversionEntity#getConversionQuery()}
 */
@Slf4j
@AllArgsConstructor
public class HiveQueryExecutionWriter implements DataWriter<QueryBasedHiveConversionEntity> {

  private final HiveJdbcConnector hiveJdbcConnector;
  private final State workUnit;

  @Override
  public void write(QueryBasedHiveConversionEntity hiveConversionEntity) throws IOException {
    List<String> conversionQueries = null;
    try {
      conversionQueries = hiveConversionEntity.getQueries();
      EventWorkunitUtils.setBeginConversionDDLExecuteTimeMetadata(this.workUnit, System.currentTimeMillis());
      this.hiveJdbcConnector.executeStatements(conversionQueries.toArray(new String[conversionQueries.size()]));
      EventWorkunitUtils.setEndConversionDDLExecuteTimeMetadata(this.workUnit, System.currentTimeMillis());
    } catch (SQLException e) {
      log.warn("Failed to execute queries: ");
      for (String conversionQuery : conversionQueries) {
        log.warn("Conversion query attempted by Hive Query writer: " + conversionQuery);
      }
      throw new IOException(e);
    }
  }

  @Override
  public void commit() throws IOException {}

  @Override
  public void close() throws IOException {
    this.hiveJdbcConnector.close();
  }

  @Override
  public void cleanup() throws IOException {}

  @Override
  public long recordsWritten() {
    return 0;
  }

  @Override
  public long bytesWritten() throws IOException {
    return 0;
  }
}