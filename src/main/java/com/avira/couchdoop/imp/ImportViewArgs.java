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
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link com.avira.couchdoop.CouchbaseArgs} implementation which holds Couchbase view import feature settings.
 */
public class ImportViewArgs extends CouchbaseArgs {

  private String designDocumentName;

  private String viewName;

  private String[] viewKeys;

  private String output;

  private int documentsPerPage;

  private int numMappers;

  public static final ArgDef ARG_DESIGNDOC_NAME = new ArgDef('d', "couchbase.designdoc.name", true, true,
    "(required) name of the design document");
  public static final ArgDef ARG_VIEW_NAME = new ArgDef('v', "couchbase.view.name", true, true,
    "(required) name of the view");
  public static final ArgDef ARG_VIEW_KEYS = new ArgDef('k', "couchbase.view.keys", true, true,
    "(required) semicolon separated list of view keys (in JSON format) which are going to be distributed to mappers");
  public static final ArgDef ARG_OUTPUT = new ArgDef('o', "output", true, true,
    "(required) HDFS output directory");
  public static final ArgDef ARG_DOCS_PER_PAGE = new ArgDef('P', "couchbase.view.docsPerPage", true, false,
    "buffer of documents which are going to be retrieved at once at a mapper; defaults to 1024");
  public static final ArgDef ARG_NUM_MAPPERS = new ArgDef('m', "hadoop.mappers", true, false,
    "number of mappers to be used by Hadoop; by default it will be equal to the number of couchbase view keys passed to the job");


  private static final char KEYS_STRING_SEPARATOR = ';';
  private static final String[] KEY_RANGE_ESCAPE_SEQUENCE = new String[]{"\\(\\(", "\\)\\)"};

  public static final List<ArgDef> ARGS_LIST = new ArrayList<>(5);

  static {
    ARGS_LIST.add(ARG_DESIGNDOC_NAME);
    ARGS_LIST.add(ARG_VIEW_NAME);
    ARGS_LIST.add(ARG_VIEW_KEYS);
    ARGS_LIST.add(ARG_OUTPUT);
    ARGS_LIST.add(ARG_DOCS_PER_PAGE);
    ARGS_LIST.add(ARG_NUM_MAPPERS);

    ARGS_LIST.addAll(CouchbaseArgs.ARGS_LIST);
  }

  public ImportViewArgs(Configuration conf) throws ArgsException {
    super(conf);
  }

  @Override
  public List<ArgDef> getArgsList() {
    return ImportViewArgs.ARGS_LIST;
  }

  @Override
  public void loadFromHadoopConfiguration(Configuration conf) throws ArgsException {
    super.loadFromHadoopConfiguration(conf);

    designDocumentName = conf.get(ARG_DESIGNDOC_NAME.getPropertyName());
    viewName = conf.get(ARG_VIEW_NAME.getPropertyName());
    viewKeys = parseViewKeys(conf);
    output = conf.get(ARG_OUTPUT.getPropertyName());
    documentsPerPage = conf.getInt(ARG_DOCS_PER_PAGE.getPropertyName(), 1024);
    //numMappers default to the number of viewKeys
    numMappers = conf.getInt(ARG_NUM_MAPPERS.getPropertyName(), viewKeys.length);
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

  public int getNumMappers() {
    return numMappers;
  }

  protected static String[] parseViewKeys(Configuration hadoopConf) {
    return parseViewKeys(hadoopConf.get(ARG_VIEW_KEYS.getPropertyName()));
  }


  protected static String[] parseViewKeys(String viewKeysString) {
    List<String> splits = splitViewKeys(viewKeysString);
    List<String> keys = new ArrayList<>(splits.size());

    //Look for KEY_RANGE_ESCAPE_SEQUENCE in each split
    Pattern multiKeyPattern = Pattern.compile(
      "(.*)" + KEY_RANGE_ESCAPE_SEQUENCE[0] + "(\\d+)-(\\d+)" + KEY_RANGE_ESCAPE_SEQUENCE[1] + "(.*)"
    );
    for (String split : splits) {
      Matcher kMatch = multiKeyPattern.matcher(split);
      if (kMatch.matches()) {
        keys.addAll(getKeysByRange(kMatch.group(1), kMatch.group(2), kMatch.group(3), kMatch.group(4)));
      } else {
        //If we did not find KEY_RANGE_ESCAPE_SEQUENCE, use the split as it is
        keys.add(split);
      }
    }

    return keys.toArray(new String[keys.size()]);
  }


  /**
   * Correctly split a string containing Couchbase view keys. Uses {@code KEYS_STRING_SEPARATOR} to split the
   * string, but not if the separator is found between double-quotes or square braces.
   *
   * @param viewKeysString string containing all Couchbase keys separated by {@code KEYS_STRING_SEPARATOR}
   * @return all Couchbase keys
   */
  protected static List<String> splitViewKeys(String viewKeysString) {
    List<String> splits = new ArrayList<>();
    char[] chars = viewKeysString.toCharArray();

    boolean betweenSquareBraces = false;
    boolean betweenQuotes = false;
    int lastMatch = 0;

    for (int i = 0; i < chars.length; i++) {
      if ((chars[i] == KEYS_STRING_SEPARATOR) && !betweenQuotes && !betweenSquareBraces) {
        splits.add(viewKeysString.substring(lastMatch, i));
        lastMatch = i + 1;
      }

      if ((chars[i] == '"') && (i == 0 || chars[i - 1] != '\\')) {
        betweenQuotes = !betweenQuotes; //toggle betweenQuotes
      }

      if ((chars[i] == '[') && (i == 0 || chars[i - 1] != '\\')) {
        betweenSquareBraces = true;
      }

      if ((chars[i] == ']') && (i == 0 || chars[i - 1] != '\\')) {
        betweenSquareBraces = false;
      }
    }

    //add last key if needed
    if (lastMatch < chars.length) {
      splits.add(viewKeysString.substring(lastMatch, chars.length));
    }

    return splits;
  }


  /**
   * Automatically generate keys containing a numeric range. Autodetect padding necessity if
   * {@code rangeStart} and {@code rangeEnd} have the same length
   *
   * @param prefix     part of key that comes before the numeric range
   * @param rangeStart start of the numeric range; must be parsable as int
   * @param rangeEnd   end of the numeric range; must be parsable as int
   * @param suffix     part of the key that comes after the numeric range
   * @return all generated keys
   */
  protected static List<String> getKeysByRange(String prefix, String rangeStart, String rangeEnd, String suffix) {
    int rangeStartInt = Integer.parseInt(rangeStart);
    int rangeEndInt = Integer.parseInt(rangeEnd);

    String numberFormat;
    if (rangeStart.length() == rangeEnd.length()) {
      numberFormat = "%0" + rangeEnd.length() + "d";
    } else {
      numberFormat = "%d";
    }

    List<String> keys = new ArrayList<>();
    for (int i = rangeStartInt; i <= rangeEndInt; i++) {
      keys.add(prefix + String.format(numberFormat, i) + suffix);
    }

    return keys;
  }

  public String getOutput() {
    return output;
  }

  public int getDocumentsPerPage() {
    return documentsPerPage;
  }
}
