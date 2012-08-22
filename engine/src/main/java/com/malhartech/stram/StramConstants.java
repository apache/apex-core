/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

/**
 * 
 * Placeholder for constants to be used by Stram/Dag<p>
 * <br>
 */
public class StramConstants {

  public static final String APPNAME = "Stram";
  
  /**
   * Environment key name pointing to the shell script's location
   */
  public static final String DISTRIBUTEDSHELLSCRIPTLOCATION = "DISTRIBUTEDSHELLSCRIPTLOCATION";

  /**
   * Environment key name denoting the file timestamp for the shell script. 
   * Used to validate the local resource. 
   */
  public static final String DISTRIBUTEDSHELLSCRIPTTIMESTAMP = "DISTRIBUTEDSHELLSCRIPTTIMESTAMP";

  /**
   * Environment key name denoting the file content length for the shell script. 
   * Used to validate the local resource. 
   */
  public static final String DISTRIBUTEDSHELLSCRIPTLEN = "DISTRIBUTEDSHELLSCRIPTLEN";
    
}
