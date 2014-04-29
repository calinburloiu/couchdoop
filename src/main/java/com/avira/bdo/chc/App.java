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

package com.avira.bdo.chc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//import com.avira.bdo.commons.mapreduce.AviraRunner;

public class App /*extends AviraRunner*/ {
//  public static void main(String[] args) throws Exception {
//    App app = new App();
//    app.start(args);
//  }
//
//  @Override
//  protected Job configureJob(Configuration conf, String input, String output) throws IOException {
//    Job job = new Job(conf);
//    job.setJarByClass(App.class);
//    job.setJobName("Untitled");
//
//    FileInputFormat.setInputPaths(job, input);
//    job.setInputFormatClass(TextInputFormat.class);
//
//    job.setMapperClass(Mapper.class);
//    job.setMapOutputKeyClass(LongWritable.class);
//    job.setMapOutputValueClass(Text.class);
//
//    job.setCombinerClass(Reducer.class);
//
//    job.setReducerClass(Reducer.class);
//    job.setOutputKeyClass(LongWritable.class);
//    job.setOutputValueClass(Text.class);
//
//    job.setOutputFormatClass(TextOutputFormat.class);
//    FileOutputFormat.setOutputPath(job, new Path(output));
//
//    return job;
//  }
}
