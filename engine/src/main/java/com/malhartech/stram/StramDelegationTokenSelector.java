/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import java.util.Collection;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class StramDelegationTokenSelector implements TokenSelector<StramDelegationTokenIdentifier>
{

  private static final Logger LOG= LoggerFactory.getLogger(StramDelegationTokenSelector.class);

  @Override
  public Token<StramDelegationTokenIdentifier> selectToken(Text text, Collection<Token<? extends TokenIdentifier>> clctn)
  {
    Token<StramDelegationTokenIdentifier> token = null;
    System.out.println("text " + text);
    if (text  != null) {
      for (Token<? extends TokenIdentifier> ctoken : clctn) {
       System.out.println("TOKEN check " + ctoken.getKind() + " " + ctoken.getService() + " " + text);
        if (StramDelegationTokenIdentifier.IDENTIFIER_KIND.equals(ctoken.getKind()) && text.equals(ctoken.getService()))
        {
          token = (Token<StramDelegationTokenIdentifier>)ctoken;
        }
      }
    }
    return token;
  }

}
