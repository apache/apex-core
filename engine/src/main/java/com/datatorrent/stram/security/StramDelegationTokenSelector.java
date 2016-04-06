/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.security;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;

/**
 * <p>StramDelegationTokenSelector class.</p>
 *
 * @since 0.3.2
 */
public class StramDelegationTokenSelector implements TokenSelector<StramDelegationTokenIdentifier>
{

  private static final Logger LOG = LoggerFactory.getLogger(StramDelegationTokenSelector.class);

  @Override
  public Token<StramDelegationTokenIdentifier> selectToken(Text text, Collection<Token<? extends TokenIdentifier>> clctn)
  {
    Token<StramDelegationTokenIdentifier> token = null;
    if (text != null) {
      for (Token<? extends TokenIdentifier> ctoken : clctn) {
        if (StramDelegationTokenIdentifier.IDENTIFIER_KIND.equals(ctoken.getKind()) && text.equals(ctoken.getService())) {
          token = (Token<StramDelegationTokenIdentifier>)ctoken;
        }
      }
    }
    return token;
  }

}
