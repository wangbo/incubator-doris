// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.load.loadv2.dpp;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.load.loadv2.BitmapValue;
import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

public abstract class SparkRDDAggregator<T> implements Serializable {

    abstract T aggregate(T v1, T v2);

    abstract byte[] serialize(Object value);

    T buildValue(Object value) {
        return (T) value;
    }

    public static SparkRDDAggregator buildAggregator(EtlJobConfig.EtlColumn column) {
        String aggType = StringUtils.lowerCase(column.aggregationType);
        switch (aggType) {
            case "bitmap_union" :
                return new BitmapUnionAggregator();
            case "hll_union" :
                return new HllUnionAggregator();
            // TODO: make more type aggregator
            // replace ,sum,max,min
            default:
                throw new RuntimeException(String.format("unsupported aggregate type %s", aggType));
        }
    }

}

class EncodeMapFunction implements PairFunction<Row, Object[], Object[]> {

    private SparkRDDAggregator[] valueAggregators;
    // include bucket id
    private int keyLen;

    public EncodeMapFunction(SparkRDDAggregator[] valueAggregators, int keyLen) {
        this.valueAggregators = valueAggregators;
        this.keyLen = keyLen;
    }

    @Override
    public Tuple2<Object[], Object[]> call(Row row) throws Exception {
        Object[] keys = new Object[keyLen];
        Object[] values = new Object[valueAggregators.length];

        for (int i = 0; i < row.size(); i++) {
            if (i < keyLen) {
                keys[i] = row.get(i);
            } else {
                int valueIdx = i - keyLen;
                values[valueIdx] = valueAggregators[valueIdx].buildValue(row.get(i));
            }
        }
        return new Tuple2<>(keys, values);
    }
}

class AggregateReduceFunction implements Function2<Object[], Object[], Object[]> {

    private SparkRDDAggregator[] valueAggregators;

    public AggregateReduceFunction(SparkRDDAggregator[] sparkDppAggregators) {
        this.valueAggregators = sparkDppAggregators;
    }

    @Override
    public Object[] call(Object[] v1, Object[] v2) throws Exception {
        Object[] result = new Object[valueAggregators.length];
        for (int i = 0; i < v1.length; i++) {
            result[i] = valueAggregators[i].aggregate(v1[i], v2[i]);
        }
        return result;
    }
}



class BitmapUnionAggregator extends SparkRDDAggregator<BitmapValue> {

    @Override
    BitmapValue aggregate(BitmapValue v1, BitmapValue v2) {
        BitmapValue newBitmapValue = new BitmapValue();
        if (v1 != null) {
            newBitmapValue.or(v1);
        }
        if (v2 != null) {
            newBitmapValue.or(v2);
        }
        return newBitmapValue;
    }

    @Override
    byte[] serialize(Object value) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream outputStream = new DataOutputStream(bos);
            ((BitmapValue)value).serialize(outputStream);
            return bos.toByteArray();
        } catch (IOException ioException) {
            ioException.printStackTrace();
            throw new RuntimeException(ioException);
        }
    }

    @Override
    BitmapValue buildValue(Object value) {
        try {
            BitmapValue bitmapValue = new BitmapValue();
            if (value instanceof byte[]) {
                bitmapValue.deserialize(new DataInputStream(new ByteArrayInputStream((byte[]) value)));
            } else {
                bitmapValue.add(value == null ? 0 : Long.valueOf(value.toString()));
            }
            return bitmapValue;
        } catch (Exception e) {
            throw new RuntimeException("build bitmap value failed", e);
        }
    }
}

class HllUnionAggregator extends SparkRDDAggregator<Hll> {

    @Override
    byte[] serialize(Object value) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream outputStream = new DataOutputStream(bos);
            ((Hll)value).serialize(outputStream);
            return bos.toByteArray();
        } catch (IOException ioException) {
            ioException.printStackTrace();
            throw new RuntimeException(ioException);
        }
    }

    @Override
    Hll aggregate(Hll v1, Hll v2) {
        Hll newHll = new Hll();
        if (v1 != null) {
            newHll.merge(v1);
        }
        if (v2 != null) {
            newHll.merge(v2);
        }
        return newHll;
    }

    @Override
    Hll buildValue(Object value) {
        try {
            Hll hll = new Hll();
            if (value instanceof byte[]) {
                hll.deserialize(new DataInputStream(new ByteArrayInputStream((byte[]) value)));
            } else {
                hll.updateWithHash(value == null ? 0 : value);
            }
            return hll;
        } catch (Exception e) {
            throw new RuntimeException("build hll value failed", e);
        }
    }

}