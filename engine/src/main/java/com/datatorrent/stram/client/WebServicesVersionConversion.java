/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import com.datatorrent.stram.webapp.WebServices;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.ClientFilter;
import java.io.ByteArrayInputStream;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class WebServicesVersionConversion
{
  public static interface Converter
  {
    public String convertCommandPath(String path);

    public String convertResponse(String path, String response);

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
      }
      catch (Exception ex) {
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
    int majorVersion = Integer.valueOf((version.split("\\."))[0]);
    int thisVersion = Integer.valueOf(WebServices.VERSION);
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
    return null;
  }

}
