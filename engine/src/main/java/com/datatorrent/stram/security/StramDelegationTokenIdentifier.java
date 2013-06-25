/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.security;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

/**
 *
 * @author Pramod Immaneni <pramod@datatorrent.com>
 */
public class StramDelegationTokenIdentifier extends AbstractDelegationTokenIdentifier
{

  public static final Text IDENTIFIER_KIND = new Text("STRAM_DELEGATION_TOKEN");

  public StramDelegationTokenIdentifier() {

  }

  public StramDelegationTokenIdentifier(Text owner, Text renewer, Text realUser) {
    super(owner, renewer, realUser);
  }

  @Override
  public Text getKind()
  {
    return IDENTIFIER_KIND;
  }

}
