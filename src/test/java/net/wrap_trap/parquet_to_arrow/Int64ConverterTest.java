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
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import org.junit.Test;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.List;

public class Int64ConverterTest {
    @Test
    public void nationKeyTest() throws IOException {
        long[] expectations = new long[]{0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L, 23L, 24L};

        Configuration conf = new Configuration();
        Path inPath = new Path("src/test/resources/nationsSF.parquet");
        ParquetMetadata metaData = ParquetFileReader.readFooter(conf, inPath);
        MessageType schema = metaData.getFileMetaData().getSchema();
        List<ColumnDescriptor> columns = schema.getColumns();
        ColumnDescriptor column = columns.get(0);
        Int64Converter converter = new Int64Converter(conf, metaData, schema, inPath, column);
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        FieldVector vector = converter.convert(allocator);

        ValueVector.Accessor accessor = vector.getAccessor();
        for (int i = 0; i < accessor.getValueCount(); i++) {
            assertThat(accessor.getObject(i), is(expectations[i]));
        }
    }

    @Test
    public void regionKeyTest() throws IOException {
        long[] expectations = new long[]{0L,1L,1L,1L,4L,0L,3L,3L,2L,2L,4L,4L,2L,4L,0L,0L,0L,1L,2L,3L,4L,2L,3L,3L,1L};

        Configuration conf = new Configuration();
        Path inPath = new Path("src/test/resources/nationsSF.parquet");
        ParquetMetadata metaData = ParquetFileReader.readFooter(conf, inPath);
        MessageType schema = metaData.getFileMetaData().getSchema();
        List<ColumnDescriptor> columns = schema.getColumns();
        ColumnDescriptor column = columns.get(2);
        Int64Converter converter = new Int64Converter(conf, metaData, schema, inPath, column);
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        FieldVector vector = converter.convert(allocator);

        ValueVector.Accessor accessor = vector.getAccessor();
        for (int i = 0; i < accessor.getValueCount(); i++) {
            assertThat(accessor.getObject(i), is(expectations[i]));
        }
    }
}
