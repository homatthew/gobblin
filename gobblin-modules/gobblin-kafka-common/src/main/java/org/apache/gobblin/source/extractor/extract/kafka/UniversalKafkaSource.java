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

package org.apache.gobblin.source.extractor.extract.kafka;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.eventbus.EventBus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.InfiniteSource;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.extract.EventBasedExtractor;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.stream.WorkUnitChangeEvent;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.gobblin.util.LoggingUncaughtExceptionHandler;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

import static java.lang.Math.*;


/**
 * A {@link KafkaSource} to use with arbitrary {@link EventBasedExtractor}. Specify the extractor to use with key
 * {@link #EXTRACTOR_TYPE}.
 */
@Slf4j
public class UniversalKafkaSource<S, D> extends KafkaSource<S, D> implements InfiniteSource<S, D> {

  public static final String EXTRACTOR_TYPE = "gobblin.source.kafka.extractorType";
  private final EventBus eventBus = new EventBus(this.getClass().getSimpleName());

  @Override
  public Extractor<S, D> getExtractor(WorkUnitState state) throws IOException {
    Preconditions.checkArgument(state.contains(EXTRACTOR_TYPE), "Missing key " + EXTRACTOR_TYPE);

    try {
      ClassAliasResolver<EventBasedExtractor> aliasResolver = new ClassAliasResolver<>(EventBasedExtractor.class);
      Class<? extends EventBasedExtractor> klazz = aliasResolver.resolveClass(state.getProp(EXTRACTOR_TYPE));

      return GobblinConstructorUtils.invokeLongestConstructor(klazz, state);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    log.info("mho-ufk-test: Hello world inside {getWorkUnits}");
    List<WorkUnit> workUnits = super.getWorkunits(state);

    WorkUnit experimentalSubject = workUnits.get(0);
    log.info("mho-ufk-test: Observing {}", experimentalSubject);
    Thread idleProcessThread = new Thread(() -> submitAfterSomeTime(this, experimentalSubject));
    //Log uncaught exceptions (e.g.OOMEs) to prevent threads from dying silently
    idleProcessThread.setName("mho-ufk-test");
    idleProcessThread.setUncaughtExceptionHandler(new LoggingUncaughtExceptionHandler(Optional.absent()));
    idleProcessThread.start();

    return workUnits;
  }

  public static void submitAfterSomeTime(UniversalKafkaSource src, WorkUnit wu) {
    try {
      /**
       * 3 scenarios to test:
       * a) remove 1, add 2 +1
       * b) remove 1, add 2 +1
       * c) remove 2, add 1 -1
       * d) remove 1, add 2 +1
       * e) remove 4, add 1 -3
       */
      Random rand = new Random();
      Thread.sleep(600000); // 10 min

      List<WorkUnit> workUnits = new ArrayList<>();
      workUnits.add(wu);
      int i = 0;
      updateWorkUnits(1, 2, workUnits, src, rand, ++i); // 2
      updateWorkUnits(1, 2, workUnits, src, rand, ++i); // 3
      updateWorkUnits(2, 1, workUnits, src, rand, ++i); // 2
      updateWorkUnits(1, 2, workUnits, src, rand, ++i); // 3
      updateWorkUnits(3, 1, workUnits, src, rand, ++i); // 1
    } catch (InterruptedException e) {
      log.info("thread dead");
      Thread.currentThread().interrupt();
    }
  }

  private static void updateWorkUnits(int sub, int add, List<WorkUnit> workUnits, UniversalKafkaSource src, Random rand,
      int count)
      throws InterruptedException {
    log.info("removing {}, and adding {} units stage={}, currentWorkUnitIds={}", sub, add, count,
        workUnits.stream().map(WorkUnit::getId).collect(Collectors.toList()));
    WorkUnit sut = workUnits.get(0);
    List<WorkUnit> workUnitsRemoved = new ArrayList();
    for (int i = 0; i < sub; i++) {
      workUnitsRemoved.add(workUnits.remove(0));
    }

    List<WorkUnit> replacementWorkUnits = new ArrayList<>();
    for (int i = 0; i < add; i++) {
      WorkUnit replacement = copyIt(sut, rand);
      replacementWorkUnits.add(replacement);
      workUnits.add(replacement);
    }

    List<String> oldTaskIds = workUnitsRemoved.stream().map(WorkUnit::getId).collect(Collectors.toList());
    log.info("workUnitUpdate oldTaskIds={}, replacementWorkUnits={}", oldTaskIds, replacementWorkUnits);
    src.onWorkUnitUpdate(oldTaskIds, replacementWorkUnits);
    Thread.sleep(600000); // 10 min
    log.info("10 minutes elapsed, stage={}", count);
  }

  private static WorkUnit copyIt(WorkUnit wu, Random rand) {
    WorkUnit copy = WorkUnit.copyOf(wu);
    copy.setId(JobLauncherUtils.newTaskId(JobLauncherUtils.newJobId("KafkaHdfsStreamingTrackingOrcTest"),
        abs(rand.nextInt())));
    return copy;
  }

  public void onWorkUnitUpdate(List<String> oldTaskIds, List<WorkUnit> newWorkUnits) {
    if (this.eventBus != null) {
      log.info("post workunit change event");
      this.eventBus.post(new WorkUnitChangeEvent(oldTaskIds, newWorkUnits));
    } else {
      log.info("event bus is null");
    }
  }

  @Override
  public EventBus getEventBus() {
    return this.eventBus;
  }
}
