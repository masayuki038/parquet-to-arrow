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

import org.apache.arrow.vector.VectorSchemaRoot;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.arrow.vector.FieldVector;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ParquetToArrowConverterTest extends ConverterTest {

    private VectorSchemaRoot result;

    @Before
    public void before() throws IOException {
        ParquetToArrowConverter converter = new ParquetToArrowConverter();
        this.result = converter.convertToArrow(TEST_FILE);
    }

    @Test
    public void parquetToArrowConverterTest() throws IOException {
        assertThat(this.result.getRowCount(), is(5));
    }

    @Test
    public void binaryConverterTest() throws IOException {
        FieldVector fieldVector = this.result.getFieldVectors().get(TestParquetFileGenerator.BINARY_FIELD_INDEX);
        assertThat(fieldVector.getValueCount(), is(5));
        for (int i = 0; i < fieldVector.getValueCount(); i++) {
            assertThat(fieldVector.getObject(i).toString(), is("foobar" + i));
        }
    }

    @Test
    public void DoubleConverterTest() throws IOException {
        FieldVector fieldVector = this.result.getFieldVectors().get(TestParquetFileGenerator.DOUBLE_FIELD_INDEX);
        assertThat(fieldVector.getValueCount(), is(5));
        for (int i = 0; i < fieldVector.getValueCount(); i++) {
            assertThat(fieldVector.getObject(i), is(2.0d + i));
        }
    }

    @Test
    public void FloatConverterTest() throws IOException {
        FieldVector fieldVector = this.result.getFieldVectors().get(TestParquetFileGenerator.FLOAT_FIELD_INDEX);
        assertThat(fieldVector.getValueCount(), is(5));
        for (int i = 0; i < fieldVector.getValueCount(); i++) {
            assertThat(fieldVector.getObject(i), is(1.0f + i));
        }
    }

    @Test
    public void Int32ConverterTest() throws IOException {
        FieldVector fieldVector = this.result.getFieldVectors().get(TestParquetFileGenerator.INT32_FIELD_INDEX);
        assertThat(fieldVector.getValueCount(), is(5));
        for (int i = 0; i < fieldVector.getValueCount(); i++) {
            assertThat(fieldVector.getObject(i), is(32 + i));
        }
    }

    @Test
    public void Int64ConverterTest() throws IOException {
        FieldVector fieldVector = this.result.getFieldVectors().get(TestParquetFileGenerator.INT64_FIELD_INDEX);
        assertThat(fieldVector.getValueCount(), is(5));
        for (int i = 0; i < fieldVector.getValueCount(); i++) {
            assertThat(fieldVector.getObject(i), is(64L + i));
        }
    }

    @Test
    public void TimestampConverterTest() throws IOException, ParseException {
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date date = sdf.parse("2018-11-04 21:41:15.123");

        FieldVector fieldVector = this.result.getFieldVectors().get(TestParquetFileGenerator.TIMESTAMP_FIELD_INDEX);
        assertThat(fieldVector.getValueCount(), is(5));
        for (int i = 0; i < fieldVector.getValueCount(); i++) {
            assertThat(fieldVector.getObject(i), is(date.getTime() + (i * 1000)));
        }
    }
}
