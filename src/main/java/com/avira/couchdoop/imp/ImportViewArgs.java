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
import com.avira.couchdoop.CouchbaseArgs;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link com.avira.couchdoop.CouchbaseArgs} implementation which holds Couchbase view import feature settings.
 */
public class ImportViewArgs extends CouchbaseArgs {

  private String designDocumentName;

  private String viewName;

  private String[] viewKeys;

  private String output;

  private int documentsPerPage;

  public static final ArgDef ARG_DESIGNDOC_NAME = new ArgDef('d', "couchbase.designDoc.name");
  public static final ArgDef ARG_VIEW_NAME = new ArgDef('v', "couchbase.view.name");
  public static final ArgDef ARG_VIEW_KEYS = new ArgDef('k', "couchbase.view.keys");
  public static final ArgDef ARG_OUTPUT = new ArgDef('o', "output");
  public static final ArgDef ARG_DOCS_PER_PAGE = new ArgDef('P', "couchbase.view.docsPerPage");

  private static final char KEYS_STRING_SEPARATOR = ';';

  public ImportViewArgs(Configuration hadoopConfiguration) throws ArgsException {
    super(hadoopConfiguration);
  }

  @Deprecated
  public ImportViewArgs(Configuration hadoopConfiguration, String[] cliArgs) throws ArgsException {
    super(hadoopConfiguration, cliArgs);
  }

  @Override
  protected Options getCliOptions() {
    Options options = super.getCliOptions();

    addOption(options, ARG_DESIGNDOC_NAME, true, true,
        "(required) name of the design document");
    addOption(options, ARG_VIEW_NAME, true, true,
        "(required) name of the view");
    addOption(options, ARG_VIEW_KEYS, true, true,
        "(required) semicolon separated list of view keys (in JSON format) which are going to be distributed to mappers");
    addOption(options, ARG_OUTPUT, true, true,
        "(required) HDFS output directory");
    addOption(options, ARG_DOCS_PER_PAGE, true, false,
        "buffer of documents which are going to be retrieved at once at a mapper; defaults to 1024");

    return options;
  }

  @Override
  public void loadHadoopConfiguration() throws ArgsException {
    super.loadHadoopConfiguration();

    designDocumentName = hadoopConfiguration.get(ARG_DESIGNDOC_NAME.getPropertyName());
    viewName = hadoopConfiguration.get(ARG_VIEW_NAME.getPropertyName());
    viewKeys = getViewKeys(hadoopConfiguration);
    output = hadoopConfiguration.get(ARG_OUTPUT.getPropertyName());

    documentsPerPage = hadoopConfiguration.getInt(ARG_DOCS_PER_PAGE.getPropertyName(), 1024);
  }

  @Override
  protected void loadCliArgsIntoHadoopConfiguration(CommandLine cl) throws ArgsException {
    super.loadCliArgsIntoHadoopConfiguration(cl);

    setPropertyFromCliArg(cl, ARG_DESIGNDOC_NAME);
    setPropertyFromCliArg(cl, ARG_VIEW_NAME);
    setPropertyFromCliArg(cl, ARG_VIEW_KEYS);
    setPropertyFromCliArg(cl, ARG_OUTPUT);
    setPropertyFromCliArg(cl, ARG_DOCS_PER_PAGE);
  }

  public String getDesignDocumentName() {
    return designDocumentName;
  }

  public String getViewName() {
    return viewName;
  }

  public String[] getViewKeys() {
    return viewKeys;
  }

  public static String[] getViewKeys(Configuration hadoopConfiguration) {
    return getViewKeys(hadoopConfiguration.get(ARG_VIEW_KEYS.getPropertyName()));
  }

  protected static String[] getViewKeys(String viewKeysParam) {
    List<String> splits = new ArrayList<>();
    char[] chars = viewKeysParam.toCharArray();

    boolean betweenSquareBraces = false;
    boolean betweenQuotes = false;
    int lastMatch = 0;

    for(int i=0;i<chars.length;i++) {
      if( (chars[i]==KEYS_STRING_SEPARATOR) && !betweenQuotes && !betweenSquareBraces ) {
        splits.add(viewKeysParam.substring(lastMatch,i));
        lastMatch = i+1;
      }

      if( (chars[i]=='"') && (i==0 || chars[i-1]!='\\') ) {
        betweenQuotes = !betweenQuotes; //toggle betweenQuotes
      }

      if( (chars[i]=='[') && (i==0 || chars[i-1]!='\\') ) {
        betweenSquareBraces = true;
      }

      if( (chars[i]==']') && (i==0 || chars[i-1]!='\\') ) {
        betweenSquareBraces = false;
      }
    }

    if(lastMatch < chars.length){
      splits.add(viewKeysParam.substring(lastMatch,chars.length));
    }

    return splits.toArray(new String[splits.size()]);
  }

  public String getOutput() {
    return output;
  }

  public int getDocumentsPerPage() {
    return documentsPerPage;
  }
}
