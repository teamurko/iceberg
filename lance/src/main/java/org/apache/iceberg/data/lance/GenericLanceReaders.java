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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.lance.ColumnVector;
import org.apache.iceberg.lance.LanceValueReader;
import org.apache.iceberg.lance.LanceValueReaders;
import org.apache.iceberg.types.Types;

public class GenericLanceReaders {
  private GenericLanceReaders() {}

  public static LanceValueReader<Record> struct(
      List<LanceValueReader<?>> readers, Types.StructType struct, Map<Integer, ?> idToConstant) {
    return new StructReader(readers, struct, idToConstant);
  }

  public static LanceValueReader<List<?>> array(LanceValueReader<?> elementReader) {
    return new ListReader(elementReader);
  }

  public static LanceValueReader<Map<?, ?>> map(
      LanceValueReader<?> keyReader, LanceValueReader<?> valueReader) {
    return new MapReader(keyReader, valueReader);
  }

  public static LanceValueReader<OffsetDateTime> timestampTzs() {
    return TimestampTzReader.INSTANCE;
  }

  public static LanceValueReader<BigDecimal> decimals() {
    return DecimalReader.INSTANCE;
  }

  public static LanceValueReader<String> strings() {
    return StringReader.INSTANCE;
  }

  public static LanceValueReader<UUID> uuids() {
    return UUIDReader.INSTANCE;
  }

  public static LanceValueReader<ByteBuffer> bytes() {
    return BytesReader.INSTANCE;
  }

  public static LanceValueReader<LocalTime> times() {
    return TimeReader.INSTANCE;
  }

  public static LanceValueReader<LocalDate> dates() {
    return DateReader.INSTANCE;
  }

  public static LanceValueReader<LocalDateTime> timestamps() {
    return TimestampReader.INSTANCE;
  }

  private static class TimestampTzReader implements LanceValueReader<OffsetDateTime> {
    public static final LanceValueReader<OffsetDateTime> INSTANCE = new TimestampTzReader();

    private TimestampTzReader() {}

    @Override
    public OffsetDateTime nonNullRead(ColumnVector vector, int row) {
      //      TimestampColumnVector tcv = (TimestampColumnVector) vector;
      //      return Instant.ofEpochSecond(Math.floorDiv(tcv.time[row], 1_000), tcv.nanos[row])
      //          .atOffset(ZoneOffset.UTC);
      return null;
    }
  }

  private static class TimeReader implements LanceValueReader<LocalTime> {
    public static final LanceValueReader<LocalTime> INSTANCE = new TimeReader();

    private TimeReader() {}

    @Override
    public LocalTime nonNullRead(ColumnVector vector, int row) {
      //      return DateTimeUtil.timeFromMicros(((LongColumnVector) vector).vector[row]);
      return null;
    }
  }

  private static class DateReader implements LanceValueReader<LocalDate> {
    public static final LanceValueReader<LocalDate> INSTANCE = new DateReader();

    private DateReader() {}

    @Override
    public LocalDate nonNullRead(ColumnVector vector, int row) {
      //      return DateTimeUtil.dateFromDays((int) ((LongColumnVector) vector).vector[row]);
      return null;
    }
  }

  private static class TimestampReader implements LanceValueReader<LocalDateTime> {
    public static final LanceValueReader<LocalDateTime> INSTANCE = new TimestampReader();

    private TimestampReader() {}

    @Override
    public LocalDateTime nonNullRead(ColumnVector vector, int row) {
      //      TimestampColumnVector tcv = (TimestampColumnVector) vector;
      //      return Instant.ofEpochSecond(Math.floorDiv(tcv.time[row], 1_000), tcv.nanos[row])
      //          .atOffset(ZoneOffset.UTC)
      //          .toLocalDateTime();
      return null;
    }
  }

  private static class DecimalReader implements LanceValueReader<BigDecimal> {
    public static final LanceValueReader<BigDecimal> INSTANCE = new DecimalReader();

    private DecimalReader() {}

    @Override
    public BigDecimal nonNullRead(ColumnVector vector, int row) {
      //      DecimalColumnVector cv = (DecimalColumnVector) vector;
      //      return cv.vector[row].getHiveDecimal().bigDecimalValue().setScale(cv.scale);
      return null;
    }
  }

  private static class StringReader implements LanceValueReader<String> {
    public static final LanceValueReader<String> INSTANCE = new StringReader();

    private StringReader() {}

    @Override
    public String nonNullRead(ColumnVector vector, int row) {
      //      BytesColumnVector bytesVector = (BytesColumnVector) vector;
      //      return new String(
      //          bytesVector.vector[row],
      //          bytesVector.start[row],
      //          bytesVector.length[row],
      //          StandardCharsets.UTF_8);
      return null;
    }
  }

  private static class UUIDReader implements LanceValueReader<UUID> {
    public static final LanceValueReader<UUID> INSTANCE = new UUIDReader();

    private UUIDReader() {}

    @Override
    public UUID nonNullRead(ColumnVector vector, int row) {
      //      BytesColumnVector bytesVector = (BytesColumnVector) vector;
      //      ByteBuffer buf =
      //          ByteBuffer.wrap(bytesVector.vector[row], bytesVector.start[row],
      // bytesVector.length[row]);
      //      return UUIDUtil.convert(buf);
      return null;
    }
  }

  private static class BytesReader implements LanceValueReader<ByteBuffer> {
    public static final LanceValueReader<ByteBuffer> INSTANCE = new BytesReader();

    private BytesReader() {}

    @Override
    public ByteBuffer nonNullRead(ColumnVector vector, int row) {
      //      BytesColumnVector bytesVector = (BytesColumnVector) vector;
      //      return ByteBuffer.wrap(
      //          bytesVector.vector[row], bytesVector.start[row], bytesVector.length[row]);
      return null;
    }
  }

  private static class StructReader extends LanceValueReaders.StructReader<Record> {
    private final GenericRecord template;

    protected StructReader(
        List<LanceValueReader<?>> readers,
        Types.StructType structType,
        Map<Integer, ?> idToConstant) {
      super(readers, structType, idToConstant);
      this.template = structType != null ? GenericRecord.create(structType) : null;
    }

    @Override
    protected Record create() {
      // GenericRecord.copy() is more performant then GenericRecord.create(StructType) since
      // NAME_MAP_CACHE access
      // is eliminated. Using copy here to gain performance.
      return template.copy();
    }

    @Override
    protected void set(Record struct, int pos, Object value) {
      struct.set(pos, value);
    }
  }

  private static class MapReader implements LanceValueReader<Map<?, ?>> {
    private final LanceValueReader<?> keyReader;
    private final LanceValueReader<?> valueReader;

    private MapReader(LanceValueReader<?> keyReader, LanceValueReader<?> valueReader) {
      this.keyReader = keyReader;
      this.valueReader = valueReader;
    }

    @Override
    public Map<?, ?> nonNullRead(ColumnVector vector, int row) {
      //      MapColumnVector mapVector = (MapColumnVector) vector;
      //      int offset = (int) mapVector.offsets[row];
      //      long length = mapVector.lengths[row];
      //      Map<Object, Object> map = Maps.newHashMapWithExpectedSize((int) length);
      //      for (int c = 0; c < length; c++) {
      //        map.put(
      //            keyReader.read(mapVector.keys, offset + c),
      //            valueReader.read(mapVector.values, offset + c));
      //      }
      //      return map;
      return null;
    }

    @Override
    public void setBatchContext(long batchOffsetInFile) {
      keyReader.setBatchContext(batchOffsetInFile);
      valueReader.setBatchContext(batchOffsetInFile);
    }
  }

  private static class ListReader implements LanceValueReader<List<?>> {
    private final LanceValueReader<?> elementReader;

    private ListReader(LanceValueReader<?> elementReader) {
      this.elementReader = elementReader;
    }

    @Override
    public List<?> nonNullRead(ColumnVector vector, int row) {
      //      ListColumnVector listVector = (ListColumnVector) vector;
      //      int offset = (int) listVector.offsets[row];
      //      int length = (int) listVector.lengths[row];
      //      List<Object> elements = Lists.newArrayListWithExpectedSize(length);
      //      for (int c = 0; c < length; ++c) {
      //        elements.add(elementReader.read(listVector.child, offset + c));
      //      }
      //      return elements;
      return null;
    }

    @Override
    public void setBatchContext(long batchOffsetInFile) {
      elementReader.setBatchContext(batchOffsetInFile);
    }
  }
}
