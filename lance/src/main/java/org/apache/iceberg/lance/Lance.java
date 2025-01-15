/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.lance;

import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class Lance {
  private Lance() {}

  public static ReadBuilder read(InputFile file) {
    return new ReadBuilder(file);
  }

  public static class ReadBuilder {
    private final InputFile file;
    private final Configuration conf;
    private Schema schema = null;
    private Long start = null;
    private Long length = null;
    private Expression filter = null;
    private boolean caseSensitive = true;
    private NameMapping nameMapping = null;

    private Function<TypeDescription, LanceRowReader<?>> readerFunc;
    //    private Function<TypeDescription, LanceBatchReader<?>> batchedReaderFunc;
    //    private int recordsPerBatch = VectorizedRowBatch.DEFAULT_SIZE;

    private ReadBuilder(InputFile file) {
      //      Preconditions.checkNotNull(file, "Input file cannot be null");
      this.file = file;
      if (file instanceof HadoopInputFile) {
        this.conf = new Configuration(((HadoopInputFile) file).getConf());
      } else {
        this.conf = new Configuration();
      }
    }

    public ReadBuilder split(long newStart, long newLength) {
      this.start = newStart;
      this.length = newLength;
      return this;
    }

    public ReadBuilder project(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    public ReadBuilder caseSensitive(boolean newCaseSensitive) {
      //      OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(this.conf, newCaseSensitive);
      this.caseSensitive = newCaseSensitive;
      return this;
    }

    public ReadBuilder config(String property, String value) {
      conf.set(property, value);
      return this;
    }

    public ReadBuilder createReaderFunc(
        Function<TypeDescription, LanceRowReader<?>> readerFunction) {
      //      Preconditions.checkArgument(
      //          this.batchedReaderFunc == null,
      //          "Reader function cannot be set since the batched version is already set");
      this.readerFunc = readerFunction;
      return this;
    }

    public ReadBuilder filter(Expression newFilter) {
      this.filter = newFilter;
      return this;
    }

    //    public ReadBuilder createBatchedReaderFunc(
    //        Function<TypeDescription, LanceBatchReader<?>> batchReaderFunction) {
    //      Preconditions.checkArgument(
    //          this.readerFunc == null,
    //          "Batched reader function cannot be set since the non-batched version is already
    // set");
    //      this.batchedReaderFunc = batchReaderFunction;
    //      return this;
    //    }

    //    public ReadBuilder recordsPerBatch(int numRecordsPerBatch) {
    //      this.recordsPerBatch = numRecordsPerBatch;
    //      return this;
    //    }

    public ReadBuilder withNameMapping(NameMapping newNameMapping) {
      this.nameMapping = newNameMapping;
      return this;
    }

    public <D> CloseableIterable<D> build() {
      Preconditions.checkNotNull(schema, "Schema is required");
      return new LanceIterable<>();
      //          file,
      //          conf,
      //          schema,
      //          nameMapping,
      //          start,
      //          length,
      //          readerFunc,
      //          caseSensitive,
      //          filter,
      //          batchedReaderFunc,
      //          recordsPerBatch);
    }
  }
}
