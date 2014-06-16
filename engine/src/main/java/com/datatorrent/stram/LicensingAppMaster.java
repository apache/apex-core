/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import java.io.StringWriter;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.log4j.DTLoggerFactory;

import com.datatorrent.stram.debug.StdOutErrLog;
import com.datatorrent.stram.license.LicensingAppMasterService;
import com.datatorrent.stram.util.VersionInfo;

/**
 * Application master for licensing
 *
 * @since 0.9.2
 */
public class LicensingAppMaster extends StramUtils.YarnContainerMain
{
  private static final Logger LOG = LoggerFactory.getLogger(LicensingAppMaster.class);
  /**
   * @param args Command line args
   * @throws Throwable
   */
  public static void main(final String[] args) throws Throwable
  {
    DTLoggerFactory.getInstance().initialize();
    StdOutErrLog.tieSystemOutAndErrToLog();
    LOG.info("Master starting with classpath: {}", System.getProperty("java.class.path"));

    LOG.info("version: {}", VersionInfo.getBuildVersion());
    StringWriter sw = new StringWriter();
    for (Map.Entry<String, String> e : System.getenv().entrySet()) {
      sw.append("\n").append(e.getKey()).append("=").append(e.getValue());
    }
    LOG.info("appmaster env:" + sw.toString());

    LicensingAppMasterService appMaster = null;
    try {
      appMaster = new LicensingAppMasterService();
      LOG.info("Initializing ApplicationMaster");

      Configuration conf = new YarnConfiguration();
      appMaster.init(conf);
      appMaster.start();
      appMaster.run();
      LOG.debug("run complete");
    }
    catch (Throwable t) {
      LOG.error("Error running ApplicationMaster", t);
      System.exit(1);
    }
    finally {
      if (appMaster != null) {
        appMaster.stop();
      }
    }

    if (appMaster != null && appMaster.getFinalStatus() == FinalApplicationStatus.SUCCEEDED) {
      LOG.info("Application Master completed. exiting");
      System.exit(0);
    }
    else {
      LOG.info("Application Master failed. exiting");
      System.exit(2);
    }
  }

  public LicensingAppMaster()
  {
  }


}
