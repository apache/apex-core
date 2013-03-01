package com.googlecode.connectlet.ssl;

import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import com.googlecode.connectlet.ssl.CertKey;
import com.googlecode.connectlet.ssl.CertMap;

public class SSLUtil {
	private static final X509TrustManager[] NULL_TRUST_MANAGERS = {
		new X509TrustManager() {
			@Override
			public void checkClientTrusted(X509Certificate[] certs, String s) {/**/}

			@Override
			public void checkServerTrusted(X509Certificate[] certs, String s) {/**/}

			@Override
			public X509Certificate[] getAcceptedIssuers() {
				return new X509Certificate[0];
			}
		}
	};

	public static SSLContext getSSLContext(CertKey certKey, CertMap certMap) {
		try {
			// 1. KeyManagerFactory
			KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
			kmf.init(certKey == null ? null : certKey.toKeyStore("", "JKS"), new char[0]);
			// 2. TrustManagerFactory
			TrustManager[] trustManagers;
			if (certMap == null) {
				trustManagers = NULL_TRUST_MANAGERS;
			} else {
				TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
				tmf.init(certMap.exportJks());
				trustManagers = tmf.getTrustManagers();
			}
			// 3. SSLContext
			SSLContext sslc = SSLContext.getInstance("TLS");
			sslc.init(kmf.getKeyManagers(), trustManagers, null);
			return sslc;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}