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

package com.avira.bdo.chc.update;

import com.avira.bdo.chc.exp.CouchbaseAction;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * {@link com.avira.bdo.chc.update.CouchbaseUpdateMapper} implementation which does the job serially without threads
 * and producer-consumer queue. It is not intended to be used in production. It is solely used as a reference in
 * benchmarking the threaded implementation.
 */
public abstract class CouchbaseSerialUpdateMapper<KEYIN, VALUEIN, T> extends CouchbaseUpdateMapper<KEYIN, VALUEIN, T> {

  @Override
  protected void map(KEYIN hKey, VALUEIN hValue, Context context) throws IOException, InterruptedException {
    // Transform the data received from the InputFormat.
    HadoopInput<T> hadoopInput = transform(hKey, hValue, context);
    if (hadoopInput == null) {
      return;
    }

    // Retrieve the current Couchbase document.
    Object couchbaseInputValue = couchbaseClient.get(hadoopInput.getCouchbaseKey());

    // Compute the Couchbase operation and the new output document.
    CouchbaseAction action = merge(hadoopInput.getHadoopData(), couchbaseInputValue, context);

    // Write the newly updated document back to Couchbase.
    context.write(hadoopInput.getCouchbaseKey(), action);
  }
}
