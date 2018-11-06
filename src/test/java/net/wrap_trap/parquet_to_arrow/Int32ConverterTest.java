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
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class Int32ConverterTest extends ConverterTest {
    @Test
    public void int32ConverterTest() throws IOException {
        FieldVectorConverter converter = build(TEST_FILE);
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        FieldVector vector = converter.convert(allocator);

        ValueVector.Accessor accessor = vector.getAccessor();
        assertThat(accessor.getValueCount(), is(5));
        for (int i = 0; i < accessor.getValueCount(); i++) {
            assertThat(accessor.getObject(i), is(32 + i));
        }
    }

    @Override
    public FieldVectorConverter createConverter(Configuration conf, Path inPath, ParquetMetadata metaData, MessageType schema, List<ColumnDescriptor> columns) {
        ColumnDescriptor column = columns.get(TestParquetFileGenerator.INT32_FIELD_INDEX);
        return new Int32Converter(conf, metaData, schema, inPath, column);
    }
}