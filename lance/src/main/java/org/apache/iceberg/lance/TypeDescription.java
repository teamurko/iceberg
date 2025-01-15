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

import java.io.Serializable;

public class TypeDescription implements Comparable<TypeDescription>, Serializable, Cloneable {
  @Override
  public int compareTo(TypeDescription o) {
    return 0;
  }

  public enum Category {
    BOOLEAN("boolean", true),
    BYTE("tinyint", true),
    SHORT("smallint", true),
    INT("int", true),
    LONG("bigint", true),
    FLOAT("float", true),
    DOUBLE("double", true),
    STRING("string", true),
    DATE("date", true),
    TIMESTAMP("timestamp", true),
    BINARY("binary", true),
    DECIMAL("decimal", true),
    VARCHAR("varchar", true),
    CHAR("char", true),
    LIST("array", false),
    MAP("map", false),
    STRUCT("struct", false),
    UNION("uniontype", false),
    TIMESTAMP_INSTANT("timestamp with local time zone", true);

    private final boolean isPrimitive;
    private final String name;

    Category(String name, boolean isPrimitive) {
      this.name = name;
      this.isPrimitive = isPrimitive;
    }

    public boolean isPrimitive() {
      return this.isPrimitive;
    }

    public String getName() {
      return this.name;
    }
  }
}
