package com.datatorrent.stram;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 1/31/14.
 *
 * @since 0.9.4
 */
public interface LicensingProtocol extends VersionedProtocol
{
  public static final long versionID = 201401310447L;

  public byte[] processRequest(byte[] request);
}
