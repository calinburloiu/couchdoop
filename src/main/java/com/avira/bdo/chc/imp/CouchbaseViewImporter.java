package com.avira.bdo.chc.imp;

import com.avira.bdo.chc.ArgsException;
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
    ImportViewArgs importViewArgs;
    importViewArgs = new ImportViewArgs(conf, args);

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

    Job job = new Job(conf);
    job.setJarByClass(CouchbaseViewImporter.class);

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
