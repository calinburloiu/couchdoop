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
import com.avira.couchdoop.ArgsHelper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Instances of this class import documents of Couchbase view keys in HDFS files.
 */
public class CouchbaseViewImporter extends Configured implements Tool {

  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseViewImporter.class);

  public void start(String[] args) throws ArgsException {
    int exitCode = 0;
    try {
      exitCode = ToolRunner.run(this, args);
    } catch (ArgsException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error(ExceptionUtils.getStackTrace(e));
    }

    System.exit(exitCode);
  }

  @Override
  public int run(String[] args) throws ArgsException {
    Configuration conf = getConf();
    ImportViewArgs importViewArgs = new ImportViewArgs(conf);

//importViewArgs.loadCliArgs(args); REPLACED BY NEXT CODE BLOCK:
    CommandLine cl = importViewArgs.parseCommandLineArgs(args);
    if (conf != null && cl != null) {
      ArgsHelper.loadClArgsIntoHadoopConf(cl, conf, ImportViewArgs.ARGS_LIST);
      importViewArgs.loadFromHadoopConfiguration(conf);
    }

    Job job;
    boolean exitStatus = true;
    try {
      job = configureJob(conf, importViewArgs.getOutput());
      exitStatus = job.waitForCompletion(true);
    } catch (Exception e) {
      LOGGER.error(ExceptionUtils.getStackTrace(e));
    }

    return exitStatus ? 0 : 2;
  }

  public Job configureJob(Configuration conf, String output) throws IOException {
    conf.setInt("mapreduce.map.failures.maxpercent", 5);
    conf.setInt("mapred.max.map.failures.percent", 5);
    conf.setInt("mapred.max.tracker.failures", 20);

    Job job = Job.getInstance(conf);
    job.setJarByClass(CouchbaseViewImporter.class);

    // User classpath takes precedence in favor of Hadoop classpath.
    // This is because the Couchbase client requires a newer version of
    // org.apache.httpcomponents:httpcore.
    job.setUserClassesTakesPrecedence(true);

    // Input
    job.setInputFormatClass(CouchbaseViewInputFormat.class);

    // Mapper
    job.setMapperClass(CouchbaseViewToFileMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    // Reducer
    job.setNumReduceTasks(0);

    // Output
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileOutputFormat.setOutputPath(job, new Path(output));

    return job;
  }
}
