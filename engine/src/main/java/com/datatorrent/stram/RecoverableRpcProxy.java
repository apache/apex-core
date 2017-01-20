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
package com.datatorrent.stram;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.List;

import javax.net.SocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;

import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol;

import static java.lang.Thread.sleep;

/**
 * Heartbeat RPC proxy invocation handler that handles fail over.
 *
 * @since 0.9.3
 */
public class RecoverableRpcProxy implements java.lang.reflect.InvocationHandler, Closeable
{
  private static final Logger LOG = LoggerFactory.getLogger(RecoverableRpcProxy.class);

  public static final String RPC_TIMEOUT = "com.datatorrent.stram.rpc.timeout";
  public static final String RETRY_TIMEOUT = "com.datatorrent.stram.rpc.retry.timeout";
  public static final String RETRY_DELAY = "com.datatorrent.stram.rpc.delay.timeout";

  public static final String QP_retryTimeoutMillis = "retryTimeoutMillis";
  public static final String QP_retryDelayMillis = "retryDelayMillis";
  public static final String QP_rpcTimeout = "rpcTimeout";

  private static final int RETRY_TIMEOUT_DEFAULT = 30000;
  private static final int RETRY_DELAY_DEFAULT = 10000;
  private static final int RPC_TIMEOUT_DEFAULT = 5000;

  private final Configuration conf;
  private StreamingContainerUmbilicalProtocol umbilical;
  private String lastConnectURI;
  private long retryTimeoutMillis;
  private long retryDelayMillis;
  private int rpcTimeout;
  private final UserGroupInformation currentUser;
  private final SocketFactory defaultSocketFactory;
  private final FSRecoveryHandler fsRecoveryHandler;

  public RecoverableRpcProxy(String appPath, Configuration conf)
  {
    this.conf = conf;
    try {
      currentUser = UserGroupInformation.getCurrentUser();
      defaultSocketFactory = NetUtils.getDefaultSocketFactory(conf);
      fsRecoveryHandler = new FSRecoveryHandler(appPath, conf);
      connect(0);
    } catch (IOException e) {
      LOG.error("Fail to create RecoverableRpcProxy", e);
      throw new RuntimeException(e);
    }
  }

  private long connect(long timeMillis) throws IOException
  {
    String uriStr = fsRecoveryHandler.readConnectUri();
    if (!uriStr.equals(lastConnectURI)) {
      LOG.debug("Got new RPC connect address {}", uriStr);
      lastConnectURI = uriStr;
      if (umbilical != null) {
        RPC.stopProxy(umbilical);
      }

      retryTimeoutMillis = Long.getLong(RETRY_TIMEOUT, RETRY_TIMEOUT_DEFAULT);
      retryDelayMillis = Long.getLong(RETRY_DELAY, RETRY_DELAY_DEFAULT);
      rpcTimeout = Integer.getInteger(RPC_TIMEOUT, RPC_TIMEOUT_DEFAULT);

      URI heartbeatUri = URI.create(uriStr);

      String queryStr = heartbeatUri.getQuery();
      if (queryStr != null) {
        List<NameValuePair> queryList = URLEncodedUtils.parse(queryStr, Charset.defaultCharset());
        if (queryList != null) {
          for (NameValuePair pair : queryList) {
            String value = pair.getValue();
            String key = pair.getName();
            if (QP_rpcTimeout.equals(key)) {
              this.rpcTimeout = Integer.parseInt(value);
            } else if (QP_retryTimeoutMillis.equals(key)) {
              this.retryTimeoutMillis = Long.parseLong(value);
            } else if (QP_retryDelayMillis.equals(key)) {
              this.retryDelayMillis = Long.parseLong(value);
            }
          }
        }
      }
      InetSocketAddress address = NetUtils.createSocketAddrForHost(heartbeatUri.getHost(), heartbeatUri.getPort());
      umbilical = RPC.getProxy(StreamingContainerUmbilicalProtocol.class, StreamingContainerUmbilicalProtocol.versionID, address, currentUser, conf, defaultSocketFactory, rpcTimeout);
      // reset timeout
      return System.currentTimeMillis() + retryTimeoutMillis;
    }
    return timeMillis;
  }

  public StreamingContainerUmbilicalProtocol getProxy() throws IOException
  {
    if (umbilical == null) {
      throw new IOException("RecoverableRpcProxy is closed.");
    }
    StreamingContainerUmbilicalProtocol recoverableProxy = (StreamingContainerUmbilicalProtocol)Proxy.newProxyInstance(umbilical.getClass().getClassLoader(), umbilical.getClass().getInterfaces(), this);
    return recoverableProxy;
  }

  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
  {
    long endTimeMillis = System.currentTimeMillis() + retryTimeoutMillis;
    if (umbilical == null) {
      endTimeMillis = connect(endTimeMillis);
    }
    while (true) {
      if (umbilical == null) {
        throw new IOException("RecoverableRpcProxy is closed.");
      }
      try {
        return method.invoke(umbilical, args);
      } catch (Throwable t) {
        // handle RPC failure
        while (t instanceof InvocationTargetException || t instanceof UndeclaredThrowableException) {
          Throwable cause = t.getCause();
          if (cause != null) {
            t = cause;
          }
        }
        final long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis < endTimeMillis) {
          LOG.warn("RPC failure, will retry after {} ms (remaining {} ms)", retryDelayMillis, endTimeMillis - currentTimeMillis, t);
          sleep(retryDelayMillis);
          endTimeMillis = connect(endTimeMillis);
        } else {
          LOG.error("Giving up RPC connection recovery after {} ms", currentTimeMillis - endTimeMillis + retryTimeoutMillis, t);
          close();
          throw t;
        }
      }
    }
  }

  @Override
  public void close()
  {
    LOG.debug("Closing RPC connection {}", lastConnectURI);
    if (umbilical != null) {
      RPC.stopProxy(umbilical);
      umbilical = null;
    }
  }

  public static URI toConnectURI(final InetSocketAddress address) throws Exception
  {
    int rpcTimeoutMillis = Integer.getInteger(RPC_TIMEOUT, RPC_TIMEOUT_DEFAULT);
    long retryDelayMillis = Long.getLong(RETRY_DELAY, RETRY_DELAY_DEFAULT);
    long retryTimeoutMillis = Long.getLong(RETRY_TIMEOUT, RETRY_TIMEOUT_DEFAULT);
    return toConnectURI(address, rpcTimeoutMillis, retryDelayMillis, retryTimeoutMillis);
  }

  public static URI toConnectURI(InetSocketAddress address, int rpcTimeoutMillis, long retryDelayMillis, long retryTimeoutMillis) throws Exception
  {
    return new URIBuilder()
        .setScheme("stram")
        .setHost(address.getHostName())
        .setPort(address.getPort())
        .setParameter(RecoverableRpcProxy.QP_rpcTimeout, Integer.toString(rpcTimeoutMillis))
        .setParameter(RecoverableRpcProxy.QP_retryDelayMillis, Long.toString(retryDelayMillis))
        .setParameter(RecoverableRpcProxy.QP_retryTimeoutMillis, Long.toString(retryTimeoutMillis))
        .build();
  }

}
