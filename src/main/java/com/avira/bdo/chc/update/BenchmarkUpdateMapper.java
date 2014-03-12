package com.avira.bdo.chc.update;

import com.avira.bdo.chc.exp.CouchbaseAction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class BenchmarkUpdateMapper extends CouchbaseSerialUpdateMapper<LongWritable, Text, Object> {

  ObjectMapper objMapper = new ObjectMapper();

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
  protected HadoopInput<Object> transform(LongWritable hKey, Text hValue) {
    return new HadoopInput<Object>(hValue.toString(), null);
  }

  @Override
  protected CouchbaseAction merge(Object o, Object cbInputValue) {
    if (cbInputValue == null) {
      throw new RuntimeException("Couchbase value is null.");
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
