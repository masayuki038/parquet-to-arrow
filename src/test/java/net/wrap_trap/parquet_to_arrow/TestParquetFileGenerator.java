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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestParquetFileGenerator {

    public static final int INT32_FIELD_INDEX = 0;
    public static final int INT64_FIELD_INDEX = 1;
    public static final int FLOAT_FIELD_INDEX = 2;
    public static final int DOUBLE_FIELD_INDEX = 3;
    public static final int BINARY_FIELD_INDEX = 4;
    public static final int TIMESTAMP_FIELD_INDEX = 5;

    public static File generateParquetFile(String path) throws IOException, ParseException {
        File f = new File(path);
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date date = sdf.parse("2018-11-04 21:41:15.123");

        Configuration conf = new Configuration();
        MessageType schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType(
                "message test { "
                + "required int32 int32_field; "
                + "required int64 int64_field; "
                + "required float float_field; "
                + "required double double_field; "
                + "required binary binary_field; "
                + "required int64 timestamp_field (TIMESTAMP_MILLIS);"
                + "} ");
        GroupWriteSupport.setSchema(schema, conf);
        SimpleGroupFactory fact = new SimpleGroupFactory(schema);
        Path fsPpath = new Path(f.getPath());
        ParquetWriter<Group> writer = new ParquetWriter<Group>(fsPpath
            , new GroupWriteSupport()
            , CompressionCodecName.UNCOMPRESSED
            , 1024
            , 1024
            , 512
            , true
            , false
            , ParquetProperties.WriterVersion.PARQUET_2_0
            , conf);
        try {
            for (int i = 0; i < 5; i++) {
                writer.write(fact.newGroup()
                    .append("int32_field", 32 + i)
                    .append("int64_field", 64L + i)
                    .append("float_field", 1.0f + i)
                    .append("double_field", 2.0d + i)
                    .append("binary_field", Binary.fromString("foobar" + i))
                    .append("timestamp_field", date.getTime() + (i * 1000)));
            }
        } finally {
            writer.close();
        }
        return f;
    }

    public static void clear(String path) {
        String[] elements = path.split("/");
        String filename = elements[elements.length - 1];
        String crc = "." + filename + ".crc";
        new File(path.replaceAll(filename, crc)).delete();
        new File(path).delete();
    }
}
