package com.datatorrent.stram.security;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.security.Principal;

/**
 * Created by pramod on 12/18/13.
 * Borrowed from
 */
public class StramWSServletRequestWrapper extends HttpServletRequestWrapper {
  private final StramWSPrincipal principal;

  public StramWSServletRequestWrapper(HttpServletRequest request, StramWSPrincipal principal) {
    super(request);
    this.principal = principal;
  }

  @Override
  public Principal getUserPrincipal() {
    return principal;
  }

  @Override
  public String getRemoteUser() {
    return principal.getName();
  }

  @Override
  public boolean isUserInRole(String role) {
    //No role info so far
    return false;
  }

}
