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

package org.apache.gobblin.data.management.dataset.filter.predicates;

import com.google.common.annotations.VisibleForTesting;
import gobblin.configuration.State;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.data.management.retention.dataset.ConfigurableCleanableDataset;
import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.hive.HiveTable;
import org.apache.gobblin.hive.metastore.HiveMetaStoreUtils;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicy;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicyBase;
import org.apache.gobblin.hive.spec.HiveSpec;
import org.apache.gobblin.util.function.CheckedExceptionPredicate;
import org.apache.hadoop.fs.Path;


/**
 * Creates {@link ConfigurableCleanableDataset} from a glob for retention jobs.
 */
@Slf4j
public class DatasetHiveSchemaContainsComplexUnion<T extends Dataset> implements CheckedExceptionPredicate<T, IOException> {
  private HiveRegistrationPolicy registrationPolicy;

  public DatasetHiveSchemaContainsComplexUnion(Properties properties) {
    this.registrationPolicy = getRegistrationPolicy(new State(properties));
  }

  @Override
  public boolean test(T dataset) throws IOException {
    Optional<HiveTable> hiveTable = getTable(dataset);
    if (!hiveTable.isPresent()) {
      log.error("No matching table for dataset={}", dataset);
    }

    return hiveTable.map(this::containsComplexUnion)
        .orElse(false);
  }

  protected Optional<HiveTable> getTable(T dataset) throws IOException {
    Path path = new Path(dataset.getUrn());
    Collection<HiveSpec> hiveSpecs = this.registrationPolicy.getHiveSpecs(path);
    return hiveSpecs.stream().map(HiveSpec::getTable).findFirst();
  }

  @VisibleForTesting
  protected boolean containsComplexUnion(HiveTable table) {
    return HiveMetaStoreUtils.containsNonOptionalUnionTypeColumn(table);
  }

  @VisibleForTesting
  protected HiveRegistrationPolicy getRegistrationPolicy(State state) {
    return HiveRegistrationPolicyBase.getPolicy(state);
  }
}
