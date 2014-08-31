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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link com.avira.couchdoop.Args} implementation which holds Couchbase connection settings.
 */
public class CouchbaseArgs extends Args {

  private List<URI> urls;
  private String bucket;
  private String password;

  public static final ArgDef ARG_COUCHBASE_URLS = new ArgDef('h', "couchbase.urls", true, true,
      "(required) comma separated URL list of one or more Couchbase nodes from the cluster");
  public static final ArgDef ARG_COUCHBASE_BUCKET = new ArgDef('b', "couchbase.bucket", true, true,
      "(required) bucket name in the cluster you wish to use");
  public static final ArgDef ARG_COUCHBASE_PASSWORD = new ArgDef('p', "couchbase.password", true, true,
      "(required) password for the bucket");

  public static final List<ArgDef> ARGS_LIST = new ArrayList<ArgDef>(3);
  static {
    ARGS_LIST.add(ARG_COUCHBASE_URLS);
    ARGS_LIST.add(ARG_COUCHBASE_BUCKET);
    ARGS_LIST.add(ARG_COUCHBASE_PASSWORD);
  }

  public CouchbaseArgs(Configuration conf) throws ArgsException {
    super(conf);
  }

  @Override
  public void loadFromHadoopConfiguration(Configuration conf) throws ArgsException {
    String rawUrls = conf.get(ARG_COUCHBASE_URLS.getPropertyName());
    if (rawUrls != null) {
      urls = new ArrayList<URI>();
      String[] urlStrings = StringUtils.split(conf.get(ARG_COUCHBASE_URLS.getPropertyName()));
      for (String urlString: urlStrings) {
        urls.add(URI.create(urlString));
      }
    }

    bucket = conf.get(ARG_COUCHBASE_BUCKET.getPropertyName(), "default");
    password = conf.get(ARG_COUCHBASE_PASSWORD.getPropertyName(), "");
  }

  @Override
  public List<ArgDef> getArgsList(){
    return CouchbaseArgs.ARGS_LIST;
  }

  public List<URI> getUrls() {
    return urls;
  }

  public String getBucket() {
    return bucket;
  }

  public String getPassword() {
    return password;
  }
}
