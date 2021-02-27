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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.apache.parquet.arrow.schema.SchemaMapping;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Provide API converting Parquet to Arrow.
 */
public class ParquetToArrow {

    /**
     * The API converting Parquet to Arrow.
     * @param parquetFilePath Parquet file path
     * @return VectorSchemaRoot of Apache Arrow
     * @throws IOException
     */
    public VectorSchemaRoot convert(String parquetFilePath) throws IOException {
        Configuration conf = new Configuration();
        Path inPath = new Path(parquetFilePath);

        ParquetMetadata metaData = ParquetFileReader.readFooter(conf, inPath);
        MessageType schema = metaData.getFileMetaData().getSchema();

        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        List<FieldVector> fieldVectorList = new ArrayList<>();

        for (ColumnDescriptor column : schema.getColumns()) {
            fieldVectorList.add(convert(conf, metaData, schema, inPath, column, allocator));
        }

        SchemaMapping schemaMapping = new SchemaConverter().fromParquet(schema);
        return new VectorSchemaRoot(schemaMapping.getArrowSchema(), fieldVectorList, fieldVectorList.get(0).getValueCount());
    }

    public VectorSchemaRoot[] convertByColumnar(String parquetFilePath) throws IOException {
        Configuration conf = new Configuration();
        Path inPath = new Path(parquetFilePath);

        ParquetMetadata metaData = ParquetFileReader.readFooter(conf, inPath);
        MessageType schema = metaData.getFileMetaData().getSchema();

        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        List<VectorSchemaRoot> rootList = new ArrayList<>();

        for (ColumnDescriptor column : schema.getColumns()) {
            List<FieldVector> fieldVectors = new ArrayList();
            List<Field> fields = new ArrayList<>();
            FieldVector fieldVector = convert(conf, metaData, schema, inPath, column, allocator);
            fields.add(fieldVector.getField());
            fieldVectors.add(fieldVector);
            rootList.add(new VectorSchemaRoot(fields, fieldVectors, fieldVector.getValueCount()));
        }
        VectorSchemaRoot[] ret = new VectorSchemaRoot[rootList.size()];
        rootList.toArray(ret);
        return ret;
    }

    protected FieldVectorConverter createFieldVectorConverter(ColumnDescriptor column, BufferAllocator allocator) {
        PrimitiveType.PrimitiveTypeName typeName = column.getType();
        switch (typeName) {
            case INT32:
                return new Int32Converter(column.getPath()[0], column.getMaxDefinitionLevel(), allocator);
            case INT64:
                return new Int64Converter(column.getPath()[0], column.getMaxDefinitionLevel(), allocator);
            case BINARY:
                return new BinaryConverter(column.getPath()[0], column.getMaxDefinitionLevel(), allocator);
            case FLOAT:
                return new FloatConverter(column.getPath()[0], column.getMaxDefinitionLevel(), allocator);
            case DOUBLE:
                return new DoubleConverter(column.getPath()[0], column.getMaxDefinitionLevel(), allocator);
            default:
                throw new UnsupportedOperationException("Unsupported Type: " + typeName);
        }
    }

    protected FieldVector convert(Configuration conf, ParquetMetadata metaData, MessageType schema, Path inPath, ColumnDescriptor column, BufferAllocator allocator) throws IOException {
        FieldVectorConverter converter = createFieldVectorConverter(column, allocator);

        try(ParquetFileReader reader =  new ParquetFileReader(conf, inPath, metaData.getBlocks(), schema.getColumns())) {
            PageReadStore store = reader.readNextRowGroup();
            while (store != null) {
                ColumnReadStoreImpl columnReadStoreImpl = new ColumnReadStoreImpl(store, new ParquetGroupConverter(), schema, "");
                int maxDefinitionLevel = column.getMaxDefinitionLevel();
                if (maxDefinitionLevel > 0) {
                    throw new UnsupportedOperationException("Only support definition level == 0");
                }
                converter.append(columnReadStoreImpl.getColumnReader(column));
                store = reader.readNextRowGroup();
            }
            return converter.buildFieldVector();
        }
    }
}
