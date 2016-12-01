/**
 * Copyright (c) 2012 - 2016 YCSB contributors. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.examples.ignite;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;

/**
 * Example to run an ignite server node.
 * 
 * @author qadahtm
 *
 */
public final class ServerNode {

  private static final String CONF_FILE_OPTION_NAME = "conffile";
  private static final String CONF_FILE_ARG_NAME = "file";
  private static final String CACHE_NAME_OPTION_NAME = "cachename";
  private static final String CACHE_NAME_ARG_NAME = "name";
  private static final String ATOMICITY_MODE_OPTION_NAME = "t";

  private ServerNode() {
  }

  public static void main(String[] args) {

    CacheConfiguration cacheCfg = null;
    IgniteConfiguration cfg = null;

    CommandLineParser cp = new DefaultParser();

    Option confFile = Option.builder(CONF_FILE_OPTION_NAME)
        .argName(CONF_FILE_ARG_NAME).hasArg().desc("use a given conffile")
        .build();

    Option cacheName = Option.builder(CACHE_NAME_OPTION_NAME)
        .argName(CACHE_NAME_ARG_NAME).hasArg().desc("use a given cache name")
        .build();

    Option atomicityMode = Option.builder(ATOMICITY_MODE_OPTION_NAME)
        .desc("use transactional atomicity mode").build();

    Options options = new Options();
    options.addOption(confFile);
    options.addOption(cacheName);
    options.addOption(atomicityMode);

    try {
      CommandLine cmd = cp.parse(options, args);
      if (cmd.hasOption(CONF_FILE_OPTION_NAME)) {
        Ignition.start(cmd.getOptionValue(CONF_FILE_OPTION_NAME));
      } else {
        cacheCfg = new CacheConfiguration<String, String>();

        if (cmd.hasOption(CACHE_NAME_OPTION_NAME)) {
          cacheCfg.setName(cmd.getOptionValue(CACHE_NAME_OPTION_NAME));
        } else {
          cacheCfg.setName("testing");
        }

        if (cmd.hasOption(ATOMICITY_MODE_OPTION_NAME)) {
          cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        }

        cfg = new IgniteConfiguration();

        cfg.setCacheConfiguration(cacheCfg);

        // Optional transaction configuration. Configure TM lookup here.
        TransactionConfiguration txCfg = new TransactionConfiguration();

        cfg.setTransactionConfiguration(txCfg);

        cfg.setPeerClassLoadingEnabled(false);

        // Start Ignite node.
        Ignition.start(cfg);
      }

    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

}
