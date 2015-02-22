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

package com.avira.couchdoop;

import com.avira.couchdoop.jobs.CouchbaseExporter;
import com.avira.couchdoop.jobs.CouchbaseViewImporter;
import com.avira.couchdoop.jobs.CouchbaseViewSerialImporter;
import com.avira.couchdoop.jobs.CouchbaseViewToHBaseImporter;

import java.util.Arrays;

/**
 * Main class for a tool which can be used to do basic data import from Couchbase to HDFS and basic data export to
 * Couchbase from HDFS.
 */
public class CouchdoopTool {

  public static final String APP_NAME = "couchdoop-tool";

  public static void main(String[] args) {
    if (args.length < 1) {
      printUsage();
      System.exit(1);
    }

    String tool = args[0];
    String[] tailArgs = Arrays.copyOfRange(args, 1, args.length);
    
    try {
      switch (tool) {
        case "import": {
          CouchbaseViewImporter importer = new CouchbaseViewImporter();
          importer.start(tailArgs);
          break;
        }
        case "import-to-hbase": {
          CouchbaseViewToHBaseImporter importer = new CouchbaseViewToHBaseImporter();
          importer.start(tailArgs);
          break;
        }
        case "serial-import": {
          CouchbaseViewSerialImporter importer = new CouchbaseViewSerialImporter();
          importer.start(tailArgs);
          break;
        }
        case "export":
          CouchbaseExporter exporter = new CouchbaseExporter();
          exporter.start(tailArgs);
          break;
        default:
          printUsage();
          break;
      }
    } catch (ArgsException e) {
      printUsage(e.getMessage());
    }
  }

  public static void printUsage() {
    printUsage(null);
  }

  public static void printUsage(String msg) {
    if (msg != null) {
      System.err.println(msg);
    }

    System.err.println("Usage:\n" +
      APP_NAME + " import [OPTIONS]\n" +
      APP_NAME + " import-to-hbase [OPTIONS]\n" +
      APP_NAME + " serial-import [OPTIONS]\n" +
      APP_NAME + " export [OPTIONS]\n");
  }
}
