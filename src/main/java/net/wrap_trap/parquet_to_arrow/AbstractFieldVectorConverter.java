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

import org.apache.arrow.vector.FieldVector;
import org.apache.parquet.column.ColumnReader;

/**
 * Provide a common function of FieldVectorConverter.
 * @param <F> The type of FieldVector that convert from Parquet
 */
public abstract class AbstractFieldVectorConverter<F extends FieldVector> implements FieldVectorConverter {

    /**
     * The instance of FieldVector that convert from Parquet.
     */
    private F fieldVector;
    /**
     * ColumnarReader index.
     */
    private int index = 0;

    /**
     * max definition level of parquet column
     */
    private int maxDefinitionLevel;

    public AbstractFieldVectorConverter(F fieldVector, int maxDefinitionLevel) {
        fieldVector.allocateNew();
        this.fieldVector = fieldVector;
        this.maxDefinitionLevel = maxDefinitionLevel;
    }

    /**
     * Add the values of ColumnReader into FieldVector.
     * @param columnReader ColumnarReader to add FieldVectorConverter
     */
    @Override
    public void append(ColumnReader columnReader) {
        long e = columnReader.getTotalValueCount();
        for (long i = 0L; i < e; i++) {
            setValues(index++, columnReader);
            columnReader.consume();
        }
    }

    /**
     * Build FieldVector.
     * @return FieldVector containing values retrieved from Parquet
     */
    @Override
    public FieldVector buildFieldVector() {
        this.fieldVector.setValueCount(this.index);
        return this.fieldVector;
    }

    protected F getFieldVector() {
        return this.fieldVector;
    }

    protected int getMaxDefinitionLevel() {
        return this.maxDefinitionLevel;
    }

    /**
     * Set the values retrieved from Parquet to FieldVector
     * @param index
     * @param columnReader
     */
    abstract protected void setValues(int index, ColumnReader columnReader);
}
