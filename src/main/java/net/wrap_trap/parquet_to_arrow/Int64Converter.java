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
import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

public class Int64Converter extends AbstractFieldVectorConverter {

    private NullableBigIntVector.Mutator mutator;

    public Int64Converter(Configuration conf, ParquetMetadata metaData, MessageType schema, Path inPath, ColumnDescriptor column) {
        super(conf, metaData, schema, inPath, column);
    }

    @Override
    protected FieldVector createFieldVector(String name, BufferAllocator allocator) {
        NullableBigIntVector vector = new NullableBigIntVector(name, allocator);
        vector.allocateNew();
        this.mutator = vector.getMutator();
        return vector;
    }

    @Override
    protected void setValue(int index, ColumnReader columnReader) {
        this.mutator.set(index, columnReader.getLong());
    }

    @Override
    protected void setValueCount(int index) {
        this.mutator.setValueCount(index);
    }
}
