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
package org.apache.iceberg.data.lance;

import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.lance.LanceRowReader;
import org.apache.iceberg.lance.LanceValueReader;
import org.apache.iceberg.lance.TypeDescription;
import org.apache.iceberg.lance.VectorizedRowBatch;

public class GenericLanceReader implements LanceRowReader<Record> {
  private final LanceValueReader<?> reader;

  public GenericLanceReader(
      Schema expectedSchema, TypeDescription readLanceSchema, Map<Integer, ?> idToConstant) {
    //    this.reader =
    //        LanceSchemaWithTypeVisitor.visit(
    //            expectedSchema, readLanceSchema, new ReadBuilder(idToConstant));
    this.reader = null;
  }

  public static LanceRowReader<Record> buildReader(
      Schema expectedSchema, TypeDescription fileSchema) {
    return new GenericLanceReader(expectedSchema, fileSchema, Collections.emptyMap());
  }

  public static LanceRowReader<Record> buildReader(
      Schema expectedSchema, TypeDescription fileSchema, Map<Integer, ?> idToConstant) {
    return new GenericLanceReader(expectedSchema, fileSchema, idToConstant);
  }

  @Override
  public Record read(VectorizedRowBatch batch, int row) {
    //    return (Record) reader.read(new StructColumnVector(batch.size, batch.cols), row);
    return null;
  }

  @Override
  public void setBatchContext(long batchOffsetInFile) {
    reader.setBatchContext(batchOffsetInFile);
  }

  //  private static class ReadBuilder extends LanceSchemaWithTypeVisitor<LanceValueReader<?>> {
  //    private final Map<Integer, ?> idToConstant;
  //
  //    private ReadBuilder(Map<Integer, ?> idToConstant) {
  //      this.idToConstant = idToConstant;
  //    }
  //
  //    @Override
  //    public LanceValueReader<?> record(
  //        Types.StructType expected,
  //        TypeDescription record,
  //        List<String> names,
  //        List<LanceValueReader<?>> fields) {
  //      return GenericLanceReaders.struct(fields, expected, idToConstant);
  //    }
  //
  //    @Override
  //    public LanceValueReader<?> list(
  //        Types.ListType iList, TypeDescription array, LanceValueReader<?> elementReader) {
  //      return GenericLanceReaders.array(elementReader);
  //    }
  //
  //    @Override
  //    public LanceValueReader<?> map(
  //        Types.MapType iMap,
  //        TypeDescription map,
  //        LanceValueReader<?> keyReader,
  //        LanceValueReader<?> valueReader) {
  //      return GenericLanceReaders.map(keyReader, valueReader);
  //    }
  //
  //    @Override
  //    public LanceValueReader<?> primitive(Type.PrimitiveType iPrimitive, TypeDescription
  // primitive) {
  //      switch (primitive.getCategory()) {
  //        case BOOLEAN:
  //          return LanceValueReaders.booleans();
  //        case BYTE:
  //          // Iceberg does not have a byte type. Use int
  //        case SHORT:
  //          // Iceberg does not have a short type. Use int
  //        case INT:
  //          return LanceValueReaders.ints();
  //        case LONG:
  //          switch (iPrimitive.typeId()) {
  //            case TIME:
  //              return GenericLanceReaders.times();
  //            case LONG:
  //              return LanceValueReaders.longs();
  //            default:
  //              throw new IllegalStateException(
  //                  String.format(
  //                      "Invalid iceberg type %s corresponding to ORC type %s",
  //                      iPrimitive, primitive));
  //          }
  //
  //        case FLOAT:
  //          return LanceValueReaders.floats();
  //        case DOUBLE:
  //          return LanceValueReaders.doubles();
  //        case DATE:
  //          return GenericLanceReaders.dates();
  //        case TIMESTAMP:
  //          return GenericLanceReaders.timestamps();
  //        case TIMESTAMP_INSTANT:
  //          return GenericLanceReaders.timestampTzs();
  //        case DECIMAL:
  //          return GenericLanceReaders.decimals();
  //        case CHAR:
  //        case VARCHAR:
  //        case STRING:
  //          return GenericLanceReaders.strings();
  //        case BINARY:
  //          switch (iPrimitive.typeId()) {
  //            case UUID:
  //              return GenericLanceReaders.uuids();
  //            case FIXED:
  //              return LanceValueReaders.bytes();
  //            case BINARY:
  //              return GenericLanceReaders.bytes();
  //            default:
  //              throw new IllegalStateException(
  //                  String.format(
  //                      "Invalid iceberg type %s corresponding to ORC type %s",
  //                      iPrimitive, primitive));
  //          }
  //        default:
  //          throw new IllegalArgumentException("Unhandled type " + primitive);
  //      }
  //    }
  //  }
}
