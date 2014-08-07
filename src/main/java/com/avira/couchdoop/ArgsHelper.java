package com.avira.couchdoop;

import com.avira.couchdoop.exp.ExportArgs;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

public class ArgsHelper {

  public static void addOption(Options options, Args.ArgDef arg, boolean hasArg,
                                  boolean isRequired, String description) {
    Option option = new Option(arg.getShortName() + "", arg.getLongName(), hasArg, description);
    option.setRequired(isRequired);
    options.addOption(option);
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

}
