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
package net.wrap_trap.parquet_to_arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.parquet.column.ColumnReader;

public class DoubleConverter extends AbstractFieldVectorConverter<Float8Vector> {

    public DoubleConverter(String name, BufferAllocator allocator) {
        super(new Float8Vector(name, allocator));
    }

    @Override
    public void setValue(int index, ColumnReader columnReader) {
        Float8Vector fieldVector = getFieldVector();
        fieldVector.set(index, columnReader.getDouble());
    }
}
