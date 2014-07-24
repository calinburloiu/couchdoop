/**
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

package com.avira.couchdoop.imp;

import com.avira.couchdoop.ArgsException;
import com.couchbase.client.protocol.views.ViewRow;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper class which maps each document retrieved from a Couchbase view key to text key-values.
 */
public class CouchbaseViewToHBaseMapper extends Mapper<Text, ViewRow, ImmutableBytesWritable, Put> {

  private String columnFamily;
  private String columnQualifier;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    try {
      ImportViewToHBaseArgs importViewToHBaseArgs = new ImportViewToHBaseArgs(context.getConfiguration());
      columnFamily = importViewToHBaseArgs.getColumnFamily();
      columnQualifier = importViewToHBaseArgs.getColumnQualifier();
      if (columnFamily == null || columnQualifier == null) {
        throw new IllegalArgumentException("Column is null.");
      }
    } catch (ArgsException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void map(Text cbKey, ViewRow cbViewRow, Context context) throws IOException, InterruptedException {
    if (cbKey != null && cbViewRow != null && cbViewRow.getDocument() != null) {
      byte[] hRowKey = Bytes.toBytes(cbKey.toString());

      Put put = new Put(hRowKey);
      put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier),
          Bytes.toBytes(cbViewRow.getDocument().toString()));

      context.write(new ImmutableBytesWritable(hRowKey), put);
    }
  }
}
