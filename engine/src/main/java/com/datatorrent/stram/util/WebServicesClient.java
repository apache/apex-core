/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.util;

import java.io.IOException;
import java.security.Principal;
import java.util.concurrent.Future;

import com.sun.jersey.api.client.AsyncWebResource;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.async.ITypeListener;
import com.sun.jersey.client.apache4.ApacheHttpClient4Handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.params.AuthPolicy;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;

/**
 * <p>WebServicesClient class.</p>
 *
 * @author Pramod Immaneni <pramod@datatorrent.com>
 * @since 0.3.2
 */
public class WebServicesClient
{

  private static final Logger LOG = LoggerFactory.getLogger(WebServicesClient.class);

  private static final PoolingClientConnectionManager connectionManager;
  private static final CredentialsProvider credentialsProvider;

  private final Client client;

  static {
    connectionManager = new PoolingClientConnectionManager();
    connectionManager.setMaxTotal(200);
    connectionManager.setDefaultMaxPerRoute(5);
    credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, new Credentials() {

      @Override
      public Principal getUserPrincipal()
      {
        return null;
      }

      @Override
      public String getPassword()
      {
        return null;
      }

    });
  }

  public WebServicesClient() {
    if (UserGroupInformation.isSecurityEnabled()) {
      DefaultHttpClient httpClient = new DefaultHttpClient(connectionManager);
      httpClient.setCredentialsProvider(credentialsProvider);
      httpClient.getAuthSchemes().register(AuthPolicy.SPNEGO, new SPNegoSchemeFactory(true));
      ApacheHttpClient4Handler httpClientHandler = new ApacheHttpClient4Handler(httpClient, new BasicCookieStore(), false);
      client = new Client(httpClientHandler);
    } else {
      client = Client.create();
    }
  }

  public WebServicesClient(Client client) {
    this.client = client;
  }

  public Client getClient() {
    return client;
  }

  public <T> T process(String url, Class<T> clazz, WebServicesHandler<T> handler) throws IOException {
    WebResource wr = client.resource(url);
    return process(wr.getRequestBuilder(), clazz, handler);
  }
  public <T> Future<T> process(String url, final ITypeListener<T> listener, WebServicesAsyncHandler<T> handler) throws IOException {
    AsyncWebResource wr = client.asyncResource(url);
    return process(wr, listener, handler);
  }

  public <T> T process(final WebResource.Builder wr, final Class<T> clazz, final WebServicesHandler<T> handler) throws IOException {
    return SecureExecutor.execute(new SecureExecutor.WorkLoad<T>(){
      @Override
      public T run()
      {
        return handler.process(wr, clazz);
      }
    });
  }

  public <T> Future<T> process(final AsyncWebResource wr, final ITypeListener<T> listener, final WebServicesAsyncHandler<T> handler) throws IOException {
    return SecureExecutor.execute(new SecureExecutor.WorkLoad<Future<T>>(){
      @Override
      public Future<T> run()
      {
        return handler.process(wr, listener);
      }
    });
  }

  /**
   *
   * @param <T>
   */
  public static abstract class WebServicesHandler<T> {
    public abstract T process(WebResource.Builder webResource, Class<T> clazz);

    @Override
    public String toString()
    {
      return "WebServicesHandler{Abstract class Useful in Future}";
    }
  }

  /**
   *
   * @param <T>
   */
  public static abstract class WebServicesAsyncHandler<T> {
    public abstract Future<T> process(AsyncWebResource webResource, ITypeListener<T> listener);

    @Override
    public String toString()
    {
      return "WebServicesAsyncHandler{Abstract class Useful in Future}";
    }
  }

  public static class GetWebServicesHandler<T> extends WebServicesHandler<T> {

    @Override
    public T process(WebResource.Builder webResource, Class<T> clazz)
    {
      return webResource.get(clazz);
    }

  }
  public static class GetWebServicesAsyncHandler<T> extends WebServicesAsyncHandler<T> {

    @Override
    public Future<T> process(AsyncWebResource webResource, ITypeListener<T> listener)
    {
      return webResource.get(listener);
    }

  }

  public static class DeleteWebServicesHandler<T> extends WebServicesHandler<T>
  {
    @Override
    public T process(WebResource.Builder webResource, Class<T> clazz)
    {
      return webResource.delete(clazz);
    }

  }

  public static class DeleteWebServicesAsyncHandler<T> extends WebServicesAsyncHandler<T>
  {
    @Override
    public Future<T> process(AsyncWebResource webResource, ITypeListener<T> listener)
    {
      return webResource.delete(listener);
    }

  }
}
