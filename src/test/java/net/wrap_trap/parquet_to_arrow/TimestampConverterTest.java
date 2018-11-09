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
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class TimestampConverterTest extends ConverterTest {

    @Test
    public void timestampConverterTest() throws IOException, ParseException {
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date date = sdf.parse("2018-11-04 21:41:15.123");

        FieldVectorConverter converter = build(TEST_FILE);
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        FieldVector vector = converter.convert(allocator);

        assertThat(vector.getValueCount(), is(5));
        for (int i = 0; i < vector.getValueCount(); i++) {
            assertThat(vector.getObject(i), is(date.getTime() + (i * 1000)));
        }
    }

    @Override
    public FieldVectorConverter createConverter(Configuration conf, Path inPath, ParquetMetadata metaData, MessageType schema,  List<ColumnDescriptor> columns) {
        ColumnDescriptor column = columns.get(TestParquetFileGenerator.TIMESTAMP_FIELD_INDEX);
        return new Int64Converter(conf, metaData, schema, inPath, column);
    }
}
