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

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Contract for JavaBeans which hold information read from the properties of a Hadoop
 * {@link org.apache.hadoop.conf.Configuration} file. This configuration in turn can be passed directly or may be read
 * from command line with Apache commons-cli library.
 */
public abstract class Args {

  private static final Logger LOGGER = LoggerFactory.getLogger(Args.class);

  /**
   * Instances of this class describe a command line argument with its short and long name which has a Java property
   * name attached.
   */
  public static class ArgDef {
    char shortName;
    String propertyName;
    boolean hasArg;
    boolean isRequired;
    String description;

    public ArgDef(char shortName, String propertyName, boolean hasArg, boolean isRequired, String description) {
      this.shortName = shortName;
      this.propertyName = propertyName;
      this.hasArg = hasArg;
      this.isRequired = isRequired;
      this.description = description;
    }

    public char getShortName() {
      return shortName;
    }

    public String getLongName() {
      return propertyName.toLowerCase().replace('.', '-');
    }

    public String getPropertyName() {
      return propertyName;
    }

    public boolean hasArg() {
      return hasArg;
    }

    public boolean isRequired() {
      return isRequired;
    }

    public String getDescription() {
      return description;
    }
  }

  public Args(Configuration conf) throws ArgsException {
    loadFromHadoopConfiguration(conf);
  }

  /**
   * Populates JavaBean instance fields with data from Hadoop configuration member.
   */
  public abstract void loadFromHadoopConfiguration(Configuration conf) throws ArgsException;

  public abstract List<ArgDef> getArgsList();

}
