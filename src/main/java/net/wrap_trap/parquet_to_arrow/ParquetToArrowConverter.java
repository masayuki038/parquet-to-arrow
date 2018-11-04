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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.apache.parquet.arrow.schema.SchemaMapping;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParquetToArrowConverter {

    public VectorSchemaRoot convertToArrow(String parquetFilePath) throws IOException {
        Configuration conf = new Configuration();
        Path inPath = new Path(parquetFilePath);

        ParquetMetadata metaData = ParquetFileReader.readFooter(conf, inPath);
        MessageType schema = metaData.getFileMetaData().getSchema();

        return convertToArrow(conf, metaData, schema, inPath);
    }

    protected VectorSchemaRoot convertToArrow(Configuration conf, ParquetMetadata metaData, MessageType schema, Path inPath) throws IOException {
        List<ColumnDescriptor> columns = schema.getColumns();

        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        List<FieldVector> fieldVectorList = new ArrayList<>();

        for (ColumnDescriptor column : columns) {
            // create ValueVector
            PrimitiveType.PrimitiveTypeName typeName = column.getType();
            switch (typeName) {
                case INT32:
                    fieldVectorList.add(new Int32Converter(conf, metaData, schema, inPath, column).convert(allocator));
                    break;
                case INT64:
                    fieldVectorList.add(new Int64Converter(conf, metaData, schema, inPath, column).convert(allocator));
                    break;
                case BINARY:
                    fieldVectorList.add(new BinaryConverter(conf, metaData, schema, inPath, column).convert(allocator));
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected type: " + typeName);
            }
        }

        SchemaMapping schemaMapping = new SchemaConverter().fromParquet(schema);
        return new VectorSchemaRoot(schemaMapping.getArrowSchema(), fieldVectorList, fieldVectorList.get(0).getAccessor().getValueCount());
    }
}
