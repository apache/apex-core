/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.security;

import com.datatorrent.stram.security.StramDelegationTokenIdentifier;

import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;

/**
 * <p>StramDelegationTokenManager class.</p>
 *
 * @author Pramod Immaneni <pramod@datatorrent.com>
 * @since 0.3.2
 */
public class StramDelegationTokenManager extends AbstractDelegationTokenSecretManager<StramDelegationTokenIdentifier>
{

  private static final long DELEGATION_KEY_UPDATE_INTERVAL = 24 * 60 * 60 * 1000;
  private static final long DELEGATION_TOKEN_MAX_LIFETIME = 7 * 24 * 60 * 60 * 1000;
  private static final long DELEGATION_TOKEN_RENEW_INTERVAL = 24 * 60 * 60 * 1000;
  private static final long DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL = 3600000;

  public StramDelegationTokenManager() {
    super(DELEGATION_KEY_UPDATE_INTERVAL, DELEGATION_TOKEN_MAX_LIFETIME, DELEGATION_TOKEN_RENEW_INTERVAL, DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL);
  }

  public byte[] addIdentifier(StramDelegationTokenIdentifier identifier) throws InvalidToken
  {
    byte[] password = retrievePassword(identifier);
    if (password == null) {
      password = createPassword(identifier);
    }
    return password;
  }

  @Override
  public StramDelegationTokenIdentifier createIdentifier()
  {
    return new StramDelegationTokenIdentifier();
  }

}
