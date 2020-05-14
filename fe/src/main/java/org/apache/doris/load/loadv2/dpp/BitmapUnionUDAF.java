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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

public class BitmapUnionUDAF extends UserDefinedAggregateFunction {
    private StructType inputSchema;
    private StructType bufferSchema;

    public BitmapUnionUDAF(DataType dataType) {
        List<StructField> inputFields = new ArrayList<>();
        inputFields.add(DataTypes.createStructField("str", dataType,true));
        inputSchema = DataTypes.createStructType(inputFields);

        List<StructField> bufferFields = new ArrayList<>();
        bufferFields.add(DataTypes.createStructField("bitmap", DataTypes.BinaryType,true));
        bufferSchema = DataTypes.createStructType(bufferFields);
    }

    @Override
    public StructType inputSchema() {
        return inputSchema;
    }

    @Override
    public StructType bufferSchema() {
        return bufferSchema;
    }

    @Override
    public DataType dataType() {
        return DataTypes.BinaryType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        RoaringBitmap roaringBitmap = new RoaringBitmap();
        buffer.update(0, serializeBitmap(roaringBitmap));
    }

    public static byte[] serializeBitmap(RoaringBitmap roaringBitmap) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream outputStream = new DataOutputStream(bos);
            outputStream.writeByte(2);
            roaringBitmap.serialize(outputStream);
            return bos.toByteArray();
        } catch (IOException ioException) {
            ioException.printStackTrace();
            throw new RuntimeException(ioException);
        }
    }

    public RoaringBitmap deserializeBitmap(byte[] byteBitmap) {
        try {
            DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(byteBitmap));
            int type = inputStream.readByte();
            assert type == 2;
            RoaringBitmap bitmap = new RoaringBitmap();
            bitmap.deserialize(inputStream);
            return bitmap;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        // here maybe there is performance problems
        Object srcValue;
        if (input.isNullAt(0)) {
            // regard null as 0
            srcValue = "0";
        } else {
            srcValue = input.get(0);
        }

        Object dstBitmapBuffer = buffer.get(0);
        byte[] dstBitmapByte = (byte[])dstBitmapBuffer;
        RoaringBitmap dstBitmap = deserializeBitmap(dstBitmapByte);

        if (srcValue instanceof String) {
            String valueStr = srcValue.toString();
            int id = Integer.parseInt(valueStr);
            dstBitmap.add(id);
        } else if (srcValue instanceof byte[]) {
            byte[] srcByte = (byte[])srcValue;
            dstBitmap.or(deserializeBitmap(srcByte));
        } else {
            throw new RuntimeException("unknown column type:" + srcValue.getClass());
        }

        buffer.update(0, serializeBitmap(dstBitmap));
    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        byte[] bitmap2Bytes = (byte[])buffer2.get(0);
        byte[] bitmap1Bytes = (byte[])buffer1.get(0);
        RoaringBitmap bitmap1 = deserializeBitmap(bitmap1Bytes);
        RoaringBitmap bitmap2 = deserializeBitmap(bitmap2Bytes);
        bitmap1.or(bitmap2);
        buffer1.update(0, serializeBitmap(bitmap1));
    }

    @Override
    public Object evaluate(Row buffer) {
        byte[] bitmapBytes = (byte[]) buffer.get(0);
        return bitmapBytes;
    }
}
