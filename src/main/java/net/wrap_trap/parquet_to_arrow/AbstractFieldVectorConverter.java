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

public abstract class AbstractFieldVectorConverter<F extends FieldVector> implements FieldVectorConverter {

    private F fieldVector;
    private int index = 0;

    public AbstractFieldVectorConverter(F fieldVector) {
        fieldVector.allocateNew();
        this.fieldVector = fieldVector;
    }

    @Override
    public void append(ColumnReader columnReader) {
        long e = columnReader.getTotalValueCount();
        for (long i = 0L; i < e; i++) {
            setValue(index++, columnReader);
            columnReader.consume();
        }
    }

    @Override
    public FieldVector buildFieldVector() {
        this.fieldVector.setValueCount(this.index);
        return this.fieldVector;
    }

    protected F getFieldVector() {
        return this.fieldVector;
    }

    abstract protected void setValue(int index, ColumnReader columnReader);
}
