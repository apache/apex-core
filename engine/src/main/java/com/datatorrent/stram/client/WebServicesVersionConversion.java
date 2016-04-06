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
package com.datatorrent.stram.client;

import java.io.ByteArrayInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.http.client.utils.URIBuilder;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.ClientFilter;

import com.datatorrent.stram.webapp.WebServices;

/**
 * <p>WebServicesVersionConversion class.</p>
 *
 * @since 0.9.2
 */
public class WebServicesVersionConversion
{
  public interface Converter
  {
    String convertCommandPath(String path);

    String convertResponse(String path, String response);

  }

  @SuppressWarnings("serial")
  public static class IncompatibleVersionException extends Exception
  {
    IncompatibleVersionException(String message)
    {
      super(message);
    }

  }

  public static class VersionConversionFilter extends ClientFilter
  {
    private final Converter converter;

    public VersionConversionFilter(Converter converter)
    {
      this.converter = converter;
    }

    @Override
    public ClientResponse handle(ClientRequest cr) throws ClientHandlerException
    {
      URIBuilder uriBuilder = new URIBuilder(cr.getURI());
      String path = uriBuilder.getPath();
      uriBuilder.setPath(converter.convertCommandPath(path));
      try {
        cr.setURI(uriBuilder.build());
        ClientResponse response = getNext().handle(cr);
        String newEntity = converter.convertResponse(path, response.getEntity(String.class));
        response.setEntityInputStream(new ByteArrayInputStream(newEntity.getBytes()));
        return response;
      } catch (Exception ex) {
        throw new ClientHandlerException(ex);
      }
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(WebServicesVersionConversion.class);

  public static boolean isVersionCompatible(String version)
  {
    if (!version.startsWith("v")) {
      LOG.error("Version {} is invalid", version);
      return false;
    }
    version = version.substring(1);
    int majorVersion = Integer.valueOf(version.split("\\.")[0]);
    int thisVersion = Integer.valueOf(WebServices.VERSION.substring(1).split("\\.")[0]);
    if (majorVersion > thisVersion) {
      LOG.warn("Future stram version {} is incompatible. Please upgrade the DataTorrent Gateway and/or CLI", majorVersion);
      return false;
    }
    // Add old versions that are not supported here in the future

    return true;
  }

  public static Converter getConverter(String version) throws IncompatibleVersionException
  {
    if (!isVersionCompatible(version)) {
      throw new IncompatibleVersionException("Stram version " + version + " is incompatible with the current build (" + WebServices.VERSION + ")");
    }
    // Add old versions that ARE supported here in the future
    if (version.equals("v1")) {
      return new Converter()
      {

        @Override
        public String convertCommandPath(String path)
        {
          return path.replaceFirst("/ws/v2/", "/ws/v1/");
        }

        @Override
        public String convertResponse(String path, String response)
        {
          return response;
        }
      };
    }
    return null;
  }

}
