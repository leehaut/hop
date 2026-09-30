/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.tableinput;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.util.List;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Stopping a pipeline must reach {@code Database.cancelQuery()} for Table Input. See <a
 * href="https://github.com/apache/hop/issues/8667">issue 8667</a>.
 */
class TableInputStopRunningTest {

  private Database db;
  private TableInputData data;
  private TableInput transform;
  private Pipeline pipeline;

  @BeforeEach
  void setUp() {
    db = mock(Database.class);
    when(db.getConnection()).thenReturn(mock(Connection.class));

    data = new TableInputData();
    data.db = db;

    TransformMeta transformMeta = mock(TransformMeta.class);
    when(transformMeta.getName()).thenReturn("Table Input");

    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    when(pipelineMeta.findTransform(anyString())).thenReturn(transformMeta);
    when(pipelineMeta.findPreviousTransforms(any(), anyBoolean())).thenReturn(List.of());
    when(pipelineMeta.findNextTransforms(any())).thenReturn(List.of());

    pipeline = new LocalPipelineEngine();
    transform =
        new TableInput(transformMeta, new TableInputMeta(), data, 0, pipelineMeta, pipeline);
  }

  @Test
  void stopRunningCancelsTheQueryWhenTheTransformIsNotStoppedYet() throws HopException {
    transform.stopRunning();

    verify(db, times(1)).cancelQuery();
    assertTrue(data.isCanceled);
    assertTrue(transform.isStopped());
  }

  /** Same sequence as {@link Pipeline#stopTransform}: mark stopped, then {@code stopRunning()}. */
  @Test
  void pipelineStopTransformCancelsTheRunningQuery() throws HopException {
    TransformMetaDataCombi tc = new TransformMetaDataCombi();
    tc.transform = transform;
    tc.data = data;

    pipeline.stopTransform(tc, false);

    verify(db, times(1)).cancelQuery();
    assertTrue(data.isCanceled);
    assertTrue(transform.isStopped());
  }
}
