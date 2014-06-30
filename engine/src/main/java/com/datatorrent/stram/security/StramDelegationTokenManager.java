/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.security;

import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;

/**
 * <p>StramDelegationTokenManager class.</p>
 *
 * @author Pramod Immaneni <pramod@datatorrent.com>
 * @since 0.3.2
 */
public class StramDelegationTokenManager extends AbstractDelegationTokenSecretManager<StramDelegationTokenIdentifier>
{

  public StramDelegationTokenManager(long delegationKeyUpdateInterval, long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
                                     long delegationTokenRemoverScanInterval) {
    super(delegationKeyUpdateInterval,delegationTokenMaxLifetime,delegationTokenRenewInterval,delegationTokenRemoverScanInterval);
  }

  @Override
  public StramDelegationTokenIdentifier createIdentifier()
  {
    return new StramDelegationTokenIdentifier();
  }

  // save the tokens

}
