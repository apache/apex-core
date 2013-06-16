/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.stram.security.StramDelegationTokenSelector;

import java.lang.annotation.Annotation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;


/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class StreamingContainerSecurityInfo extends SecurityInfo
{

  @Override
  public KerberosInfo getKerberosInfo(Class<?> type, Configuration c)
  {
    return null;
  }

  @Override
  public TokenInfo getTokenInfo(Class<?> type, Configuration c)
  {
    TokenInfo tokenInfo = null;
    if (type.equals(StreamingContainerUmbilicalProtocol.class))
    {
        tokenInfo = new TokenInfo() {

        @Override
        public Class<? extends TokenSelector<? extends TokenIdentifier>> value()
        {
          return StramDelegationTokenSelector.class;
        }

        @Override
        public Class<? extends Annotation> annotationType()
        {
          return null;
        }

      };
    }
    return tokenInfo;
  }

}
