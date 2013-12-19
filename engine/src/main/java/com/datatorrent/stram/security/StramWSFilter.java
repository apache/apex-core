package com.datatorrent.stram.security;

import com.datatorrent.stram.webapp.WebServices;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;

import javax.servlet.*;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by pramod on 12/17/13.
 * Built on org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
 * See https://issues.apache.org/jira/browse/YARN-1516
 */
public class StramWSFilter implements Filter
{
  private static final Log LOG = LogFactory.getLog(StramWSFilter.class);

  public static final String PROXY_HOST = "PROXY_HOST";
  public static final String PROXY_URI_BASE = "PROXY_URI_BASE";
  //update the proxy IP list about every 5 min
  private static final long updateInterval = 5 * 60 * 1000;

  public static final String CLIENT_COOKIE = "stram-client";

  // This will not be needed once all requests can go through the proxy
  private static final String WEBAPP_PROXY_USER = "proxy-user";

  private String proxyHost;
  private Set<String> proxyAddresses = null;
  private long lastUpdate;
  private String proxyUriBase;

  private StramDelegationTokenManager tokenManager;
  private AtomicInteger sequenceNumber;

  @Override
  public void init(FilterConfig conf) throws ServletException {
    proxyHost = conf.getInitParameter(PROXY_HOST);
    proxyUriBase = conf.getInitParameter(PROXY_URI_BASE);
    tokenManager = new StramDelegationTokenManager();
    sequenceNumber = new AtomicInteger(0);
    try {
      tokenManager.startThreads();
    } catch (IOException e) {
      throw new ServletException(e);
    }
  }

  protected Set<String> getProxyAddresses() throws ServletException {
    long now = System.currentTimeMillis();
    synchronized(this) {
      if(proxyAddresses == null || (lastUpdate + updateInterval) >= now) {
        try {
          proxyAddresses = new HashSet<String>();
          for(InetAddress add : InetAddress.getAllByName(proxyHost)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("proxy address is: " + add.getHostAddress());
            }
            proxyAddresses.add(add.getHostAddress());
          }
          lastUpdate = now;
        } catch (UnknownHostException e) {
          throw new ServletException("Could not locate "+proxyHost, e);
        }
      }
      return proxyAddresses;
    }
  }

  @Override
  public void destroy() {
    //Empty
    tokenManager.stopThreads();
  }

  @Override
  public void doFilter(ServletRequest req, ServletResponse resp,
                       FilterChain chain) throws IOException, ServletException {
    if(!(req instanceof HttpServletRequest)) {
      throw new ServletException("This filter only works for HTTP/HTTPS");
    }

    HttpServletRequest httpReq = (HttpServletRequest)req;
    HttpServletResponse httpResp = (HttpServletResponse)resp;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Remote address for request is: " + httpReq.getRemoteAddr());
    }
    String requestURI = httpReq.getRequestURI();
    LOG.info("REQ path " + requestURI);
    boolean authenticate = true;
    if(getProxyAddresses().contains(httpReq.getRemoteAddr())) {
      if (requestURI.equals(WebServices.PATH)) {
        String token = createClientToken(httpReq.getLocalAddr());
        LOG.info("Create token " + token);
        Cookie cookie = new Cookie(CLIENT_COOKIE, token);
        httpResp.addCookie(cookie);
      }
      authenticate = false;
    }
    if (authenticate) {
      Cookie clcookie = null;
      Cookie[] cookies = httpReq.getCookies();
      for (Cookie cookie : cookies) {
        if (cookie.getName().equals(CLIENT_COOKIE)) {
          clcookie = cookie;
          break;
        }
      }
      if ((clcookie == null) || !verifyClientToken(clcookie.getValue())) {
        httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        return;
      }
    }

    String user = null;

    if (httpReq.getCookies() != null) {
      for(Cookie c: httpReq.getCookies()) {
        if(WEBAPP_PROXY_USER.equals(c.getName())){
          user = c.getValue();
          break;
        }
      }
    }
    if(user == null) {
      LOG.warn("Could not find "+WEBAPP_PROXY_USER
              +" cookie, so user will not be set");
      chain.doFilter(req, resp);
    } else {
      final StramWSPrincipal principal = new StramWSPrincipal(user);
      ServletRequest requestWrapper = new StramWSServletRequestWrapper(httpReq, principal);
      chain.doFilter(requestWrapper, resp);
    }
  }

  private String createClientToken(String service) throws IOException
  {
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    String username = ugi.getUserName();
    StramDelegationTokenIdentifier tokenIdentifier = new StramDelegationTokenIdentifier(new Text(username), new Text(username), new Text());
    tokenIdentifier.setSequenceNumber(sequenceNumber.getAndAdd(1));
    byte[] password = tokenManager.addIdentifier(tokenIdentifier);
    Token<StramDelegationTokenIdentifier> token = new Token<StramDelegationTokenIdentifier>(tokenIdentifier.getBytes(), password, tokenIdentifier.getKind(), new Text(service));
    return token.encodeToUrlString();
  }

  private boolean verifyClientToken(String tokenstr) throws IOException
  {
    LOG.info("Verifying token " + tokenstr);
    boolean match = false;
    Token<StramDelegationTokenIdentifier> token = new Token<StramDelegationTokenIdentifier>();
    token.decodeFromUrlString(tokenstr);
    byte[] identifier = token.getIdentifier();
    byte[] password = token.getPassword();
    StramDelegationTokenIdentifier tokenIdentifier = new StramDelegationTokenIdentifier();
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(identifier));
    tokenIdentifier.readFields(input);
    try {
      tokenManager.verifyToken(tokenIdentifier, password);
      match = true;
    } catch (SecretManager.InvalidToken iv) {
      LOG.error("Invalid token ", iv);
    }
    LOG.info("Verified " + match);
    return match;
  }
}
