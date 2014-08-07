package com.avira.couchdoop;

import com.avira.couchdoop.exp.ExportArgs;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ArgsHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArgsHelper.class);

  public static void addOption(Options options, Args.ArgDef arg, boolean hasArg,
                                  boolean isRequired, String description) {
    Option option = new Option(arg.getShortName() + "", arg.getLongName(), hasArg, description);
    option.setRequired(isRequired);
    options.addOption(option);
  }

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

  public static void loadCliArgsIntoHadoopConf(CommandLine cl, Configuration hadoopConf,
                                               List<Args.ArgDef> argsList) {
    for (Args.ArgDef arg : argsList) {
      setPropertyFromCliArg(hadoopConf, cl, arg);
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
