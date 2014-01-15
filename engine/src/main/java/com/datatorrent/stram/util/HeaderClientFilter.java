package com.datatorrent.stram.util;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.ClientFilter;

import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MultivaluedMap;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by pramod on 12/19/13.
 *
 * @since 0.9.2
 */
public class HeaderClientFilter extends ClientFilter
{
  private static final String COOKIE_HEADER = "Cookie";

  private List<Cookie> cookies = new ArrayList<Cookie>();

  public void addCookie(Cookie cookie) {
    cookies.add(cookie);
  }

  @Override
  public ClientResponse handle(ClientRequest clientRequest) throws ClientHandlerException
  {
    final MultivaluedMap<String,Object> headers = clientRequest.getHeaders();
    List<Object> hcookies = headers.get(COOKIE_HEADER);
    if (hcookies == null) {
      hcookies = new ArrayList<Object>();
    }
    hcookies.addAll(cookies);
    headers.put(COOKIE_HEADER, hcookies);
    return getNext().handle(clientRequest);
  }
}
