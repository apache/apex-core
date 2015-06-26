/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
