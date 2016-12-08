package org.apache.apex.log.appender;

public interface LogLocationProvider
{
  /**
   * Returns current file name
   * @return
   */
  String getCurrentFileName();

  /**
   * Returns current file offiset
   * @return
   */
  long getCurrentFileOffset();
}
