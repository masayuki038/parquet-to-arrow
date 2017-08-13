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
import org.apache.arrow.vector.FieldVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

public abstract class AbstractFieldVectorConverter implements FieldVectorConverter {

    private Configuration conf;
    private ParquetMetadata metaData;
    private MessageType schema;
    private Path inPath;
    private ColumnDescriptor column;

    public AbstractFieldVectorConverter(Configuration conf, ParquetMetadata metaData, MessageType schema, Path inPath, ColumnDescriptor column) {
        this.conf = conf;
        this.metaData = metaData;
        this.schema = schema;
        this.inPath = inPath;
        this.column = column;
    }

    abstract protected FieldVector createFieldVector(String name, BufferAllocator allocator);
    abstract protected void setValue(int index, ColumnReader columnReader);
    abstract protected void setValueCount(int index);

    public FieldVector convert(BufferAllocator allocator) throws IOException {
        ParquetFileReader reader =  new ParquetFileReader(conf, inPath, metaData.getBlocks(), schema.getColumns());
        FieldVector vector = createFieldVector(column.getPath()[0], allocator);

        int index = 0;
        PageReadStore store = reader.readNextRowGroup();
        while (store != null) {
            ColumnReadStoreImpl columnReadStoreImpl = new ColumnReadStoreImpl(store, new ParquetGroupConverter(), schema, "");
            int maxDefinitionLevel = column.getMaxDefinitionLevel();
            if (maxDefinitionLevel > 0) {
                throw new UnsupportedOperationException("Only support definition level == 0");
            }

            ColumnReader columnReader = columnReadStoreImpl.getColumnReader(column);
            long e = columnReader.getTotalValueCount();
            for (long i = 0L; i < e; i++) {
                setValue(index++, columnReader);
                columnReader.consume();
            }
            store = reader.readNextRowGroup();
        }
        setValueCount(index);
        return vector;
    }
}
