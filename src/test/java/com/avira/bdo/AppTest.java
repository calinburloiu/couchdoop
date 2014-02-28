package com.avira.bdo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test for simple App.
 */
public class AppTest {
  
  MapDriver<LongWritable, Text, LongWritable, Text> mapDriver;
  ReduceDriver<LongWritable, Text, LongWritable, Text> combineDriver;
  ReduceDriver<LongWritable, Text, LongWritable, Text> reduceDriver;
  MapReduceDriver<LongWritable, Text, LongWritable, Text, LongWritable, Text> mrDriver;
  
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(AppTest.class);
  
  @Before
  public void setUp() {
    // Setup mapper.
    Mapper<LongWritable, Text, LongWritable, Text> mapper =
        new Mapper<LongWritable, Text, LongWritable, Text>();
    mapDriver = MapDriver.newMapDriver(mapper);
    
    // Setup combiner.
    Reducer<LongWritable, Text, LongWritable, Text> combiner = 
        new Reducer<LongWritable, Text, LongWritable, Text>();
    combineDriver = ReduceDriver.newReduceDriver(combiner);
    
    // Setup reducer.
    Reducer<LongWritable, Text, LongWritable, Text> reducer = 
        new Reducer<LongWritable, Text, LongWritable, Text>();
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    
    // Setup MapReduce job.
    mrDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer, combiner);
  }

  @Test
  public void testMapper() throws IOException {
    // Inputs
    mapDriver.addInput(new LongWritable(0L), new Text("Mary had a little lamb,"));
    mapDriver.addInput(new LongWritable(24L), new Text("Little lamb, little lamb."));
    
    // Outputs
    mapDriver.addOutput(new LongWritable(0L), new Text("Mary had a little lamb,"));
    mapDriver.addOutput(new LongWritable(24L), new Text("Little lamb, little lamb."));
    
    mapDriver.runTest();
  }
  
  @Test
  public void testReducer() throws IOException {
    // Inputs
    List<Text> values = new ArrayList<Text>();
    values.add(new Text("Mary had a little lamb,"));
    values.add(new Text("Little lamb, little lamb."));
    reduceDriver.addInput(new LongWritable(10L), values);
    
    // Outputs
    reduceDriver.addOutput(new LongWritable(10L), new Text("Mary had a little lamb,"));
    reduceDriver.addOutput(new LongWritable(10L), new Text("Little lamb, little lamb."));
    
    reduceDriver.runTest();
  }
  
  @Test
  public void testJob() throws IOException {
    // Inputs
    mrDriver.addInput(new LongWritable(0L), new Text("Mary had a little lamb,"));
    mrDriver.addInput(new LongWritable(24L), new Text("Little lamb, little lamb."));
    
    // Outputs
    mrDriver.addOutput(new LongWritable(0L), new Text("Mary had a little lamb,"));
    mrDriver.addOutput(new LongWritable(24L), new Text("Little lamb, little lamb."));
    
    mrDriver.runTest();
  }
  
  @After
  public void tearDown() {
    
  }
}
