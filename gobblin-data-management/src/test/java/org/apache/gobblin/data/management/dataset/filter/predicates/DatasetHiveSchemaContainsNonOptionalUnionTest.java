package org.apache.gobblin.data.management.dataset.filter.predicates;

import gobblin.configuration.State;
import java.io.IOException;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.conversion.hive.LocalHiveMetastoreTestUtils;
import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.dataset.test.SimpleDatasetForTesting;
import org.mockito.cglib.core.Local;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(groups = {"gobblin.hive"})
public class DatasetHiveSchemaContainsNonOptionalUnionTest {

  LocalHiveMetastoreTestUtils _localHiveMetastoreTestUtils;

  @BeforeClass
  public void setup() {
    _localHiveMetastoreTestUtils = LocalHiveMetastoreTestUtils.getInstance();
  }

  @Test
  public void testTest1() throws IOException {
    State state = new State();
    state.setProp(ConfigurationKeys.HIVE_REGISTRATION_POLICY, TestHiveRegistrationPolicy.class.getName());

    DatasetHiveSchemaContainsNonOptionalUnion<Dataset> containsNonOptionalUnion =
        new DatasetHiveSchemaContainsNonOptionalUnion(state);

    containsNonOptionalUnion.test(new SimpleDatasetForTesting("helloWorld"));
  }
}
