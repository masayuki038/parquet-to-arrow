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

import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.text.ParseException;


public abstract class ConverterTest {

    protected static String TEST_FILE = "src/test/resources/test.parquet";

    @BeforeClass
    public static void setUp() throws IOException, ParseException {
        //System.setProperty("hadoop.home.dir", "d:/development/hadoop");
        TestParquetFileGenerator.generateParquetFile(TEST_FILE);
    }

    @AfterClass
    public static void tearDown() {
        TestParquetFileGenerator.clear(TEST_FILE);
    }
}
