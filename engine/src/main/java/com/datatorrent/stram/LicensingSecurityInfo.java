package com.datatorrent.stram;

import com.datatorrent.stram.license.security.LicenseDelegationTokenSelector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;

import java.lang.annotation.Annotation;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 6/16/14.
 */
public class LicensingSecurityInfo extends SecurityInfo
{
  @Override
  public KerberosInfo getKerberosInfo(Class<?> type, Configuration conf)
  {
    return null;
  }

  @Override
  public TokenInfo getTokenInfo(Class<?> type, Configuration conf)
  {
    TokenInfo tokenInfo = null;
    if (type.equals(LicensingProtocol.class))
    {
      tokenInfo = new TokenInfo() {

        @Override
        public Class<? extends TokenSelector<? extends TokenIdentifier>> value()
        {
          return LicenseDelegationTokenSelector.class;
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
