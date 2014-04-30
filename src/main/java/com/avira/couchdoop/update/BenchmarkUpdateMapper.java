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

package com.avira.couchdoop.update;

import com.avira.couchdoop.exp.CouchbaseAction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class BenchmarkUpdateMapper extends CouchbaseUpdateMapper<LongWritable, Text, Object> {

  ObjectMapper objMapper = new ObjectMapper();

  public static enum Counters { NULL_DOCS }

  private static class Bean {
    private String letters;
    private Integer number;

    private Bean() {
    }

    private Bean(String letters, Integer number) {
      this.letters = letters;
      this.number = number;
    }

    public String getLetters() {
      return letters;
    }

    public void setLetters(String letters) {
      this.letters = letters;
    }

    public Integer getNumber() {
      return number;
    }

    public void setNumber(Integer number) {
      this.number = number;
    }
  }

  @Override
  protected HadoopInput<Object> transform(LongWritable hKey, Text hValue, Context context) {
    String[] splits = hValue.toString().split("\t");

    return new HadoopInput<Object>(splits[0], null);
  }

  @Override
  protected CouchbaseAction merge(Object o, Object cbInputValue, Context context) {
    if (cbInputValue == null) {
      context.getCounter(Counters.NULL_DOCS).increment(1);
      return CouchbaseAction.createNoneAction();
    }

    try {
      Bean bean = objMapper.readValue(cbInputValue.toString(), Bean.class);

      // If lower case make upper case and vice versa.
      char firstChar = bean.getLetters().charAt(0);
      if (firstChar >= 'a' && firstChar <= 'z') {
        bean.setLetters(bean.getLetters().toUpperCase());
        bean.setNumber(-Math.abs(bean.getNumber()));
      } else {
        bean.setLetters(bean.getLetters().toLowerCase());
        bean.setNumber(Math.abs(bean.getNumber()));
      }

      String outputJson = objMapper.writeValueAsString(bean);
      return CouchbaseAction.createSetAction(outputJson);
    } catch (IOException e) {
//      return CouchbaseAction.createNoneAction();
      throw new RuntimeException("Invalid JSON format.");
    }
  }
}
