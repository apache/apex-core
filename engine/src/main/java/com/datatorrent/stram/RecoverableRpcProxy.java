/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.mortbay.util.MultiMap;
import org.mortbay.util.UrlEncoded;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol;

/**
 * Heartbeat RPC proxy invocation handler that handles fail over.
 *
 * @since 0.9.3
 */
public class RecoverableRpcProxy implements java.lang.reflect.InvocationHandler, Closeable
{
  private final static Logger LOG = LoggerFactory.getLogger(RecoverableRpcProxy.class);

  public static final String QP_retryTimeoutMillis = "retryTimeoutMillis";
  public static final String QP_retryDelayMillis = "retryDelayMillis";
  public static final String QP_rpcTimeout = "rpcTimeout";

  private final Configuration conf;
  private final String appPath;
  private StreamingContainerUmbilicalProtocol umbilical;
  private String lastConnectURI;
  private long lastCompletedCallTms;
  private long retryTimeoutMillis = 30000;
  private long retryDelayMillis = 10000;
  private int rpcTimeout = 5000;

  public RecoverableRpcProxy(String appPath, Configuration conf) throws IOException
  {
    this.conf = conf;
    this.appPath = appPath;
    connect();
  }

  private void connect() throws IOException
  {
    FSRecoveryHandler fsrh = new FSRecoveryHandler(appPath, conf);
    String uriStr = fsrh.readConnectUri();
    if (!uriStr.equals(lastConnectURI)) {
      // reset timeout
      LOG.debug("Got new RPC connect address {}", uriStr);
      lastCompletedCallTms = System.currentTimeMillis();
      lastConnectURI = uriStr;
    }
    URI heartbeatUri = URI.create(uriStr);

    String queryStr = heartbeatUri.getQuery();
    MultiMap mm = new MultiMap();
    if (queryStr != null) {
      UrlEncoded.decodeTo(queryStr, mm, Charset.defaultCharset().name());
    }
    for (Object key : mm.keySet()) {
      String value = mm.getString(key);
      if (QP_rpcTimeout.equals(key)) {
        this.rpcTimeout = Integer.parseInt(value);
      } else if (QP_retryTimeoutMillis.equals(key)) {
        this.retryTimeoutMillis = Long.parseLong(value);
      } else if (QP_retryDelayMillis.equals(key)) {
        this.retryDelayMillis = Long.parseLong(value);
      }
    }
    InetSocketAddress address = NetUtils.createSocketAddrForHost(heartbeatUri.getHost(), heartbeatUri.getPort());
    umbilical = RPC.getProxy(StreamingContainerUmbilicalProtocol.class, StreamingContainerUmbilicalProtocol.versionID, address, UserGroupInformation.getCurrentUser(), conf, NetUtils.getDefaultSocketFactory(conf), rpcTimeout);
  }

  public StreamingContainerUmbilicalProtocol getProxy()
  {
    StreamingContainerUmbilicalProtocol recoverableProxy = (StreamingContainerUmbilicalProtocol)java.lang.reflect.Proxy.newProxyInstance(umbilical.getClass().getClassLoader(), umbilical.getClass().getInterfaces(), this);
    return recoverableProxy;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
  {
    Object result;
    for (;;) {
      try {
        if (umbilical == null) {
          connect();
        }
        //long start = System.nanoTime();
        result = method.invoke(umbilical, args);
        lastCompletedCallTms = System.currentTimeMillis();
        //long end = System.nanoTime();
        //LOG.info(String.format("%s took %d ns", method.getName(), (end - start)));
        return result;
      } catch (InvocationTargetException e) {
        // reconnect on error
        Throwable targetException = e.getTargetException();
        if (targetException instanceof java.net.ConnectException) {
          LOG.error("Failed to connect", targetException);
        } else if (targetException instanceof java.net.SocketTimeoutException) {
          LOG.error("RPC timeout", targetException);
        } else {
          LOG.debug("Unexpected RPC error", targetException);
        }
        long connectMillis = System.currentTimeMillis() - lastCompletedCallTms;
        if (connectMillis < retryTimeoutMillis) {
          LOG.info("RPC failure, attempting reconnect after {} ms (remaining {} ms)", retryDelayMillis, retryTimeoutMillis-connectMillis);
          umbilical = null;
          Thread.sleep(retryDelayMillis);
        } else {
          LOG.error("Giving up RPC connection recovery after {} ms", connectMillis);
          throw targetException;
        }
      } catch (Exception e) {
        throw new RuntimeException("unexpected exception: " + e.getMessage(), e);
      } finally {
      }
    }
  }

  @Override
  public void close() throws IOException
  {
    if (umbilical != null) {
      RPC.stopProxy(umbilical);
      umbilical = null;
    }
  }

  public static URI toConnectURI(InetSocketAddress address, int rpcTimeoutMillis, int retryDelayMillis, int retryTimeoutMillis) throws Exception
  {
    StringBuilder query = new StringBuilder();
    query.setLength(0);
    query.append(RecoverableRpcProxy.QP_rpcTimeout + "=" + rpcTimeoutMillis);
    query.append('&');
    query.append(RecoverableRpcProxy.QP_retryDelayMillis + "=" + retryDelayMillis);
    query.append('&');
    query.append(RecoverableRpcProxy.QP_retryTimeoutMillis + "=" + retryTimeoutMillis);
    return new URI("stram", null, address.getHostName(), address.getPort(), null, query.toString(), null);
  }


}
