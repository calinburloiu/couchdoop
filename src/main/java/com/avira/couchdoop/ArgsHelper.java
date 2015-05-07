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

import com.avira.couchdoop.exp.ExportArgs;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ArgsHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArgsHelper.class);

  public static Option getOptionFromArg(Args.ArgDef arg) {
    Option option = new Option(arg.getShortName() + "", arg.getLongName(), arg.hasArg, arg.description);
    option.setRequired(arg.isRequired);
    return option;
  }

  public static void setPropertyFromCliArg(Configuration hadoopConf, CommandLine cl, Args.ArgDef arg) {
    String argValue = cl.getOptionValue(arg.getShortName());

    if (argValue != null) {
      hadoopConf.set(arg.getPropertyName(), argValue);
    }
  }

  public static void loadCliArgsIntoHadoopConf(Configuration hadoopConf,
                                               List<Args.ArgDef> argsList, String[] cliArgs) throws ArgsException {
    Options options = getCliOptionsFromArgsList(argsList);
    CommandLine cl = ArgsHelper.parseCommandLineArgs(options, cliArgs);
    if (hadoopConf == null || cl == null) {
      throw new RuntimeException("Can't load Cli Args Into Hadoop Conf!");
    }

    for (Args.ArgDef arg : argsList) {
      setPropertyFromCliArg(hadoopConf, cl, arg);
    }
  }


  public static Options getCliOptionsFromArgsList(List<Args.ArgDef> argsList) {
    Options options = new Options();
    for (Args.ArgDef arg : argsList) {
      options.addOption(getOptionFromArg(arg));
    }
    return options;
  }


  private static CommandLine parseCommandLineArgs(Options options, String[] cliArgs) throws ArgsException {
    CommandLineParser parser = new PosixParser();
    CommandLine cl;
    try {
      cl = parser.parse(options, cliArgs);
    } catch (ParseException e) {
      LOGGER.error(e.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.setOptionComparator(null);
      formatter.printHelp("...", options);

      throw new ArgsException(e);
    }

    return cl;
  }

}
