package com.datatorrent.stram.security;

import java.security.Principal;

/**
 * Created by pramod on 12/18/13.
 * Borrowed from org.apache.hadoop.yarn.server.webproxy.amfilter.AmIPPrincipal
 * See https://issues.apache.org/jira/browse/YARN-1516
 *
 * @since 0.9.2
 */
public class StramWSPrincipal implements Principal {
  private final String name;

  public StramWSPrincipal(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }
}
