package com.googlecode.connectlet.ssl;

import com.googlecode.connectlet.Connector;
import com.googlecode.connectlet.misc.BroadcastServer;
import com.googlecode.connectlet.misc.DumpFilterFactory;
import com.googlecode.connectlet.misc.ForwardServer;
import com.googlecode.connectlet.util.ClassPath;

public class TestSSLForward {
	public static void main(String[] args) throws Exception {
		CertKey certKey = new CertKey(ClassPath.getInstance("../conf/localhost.pfx"), "changeit", "PKCS12");
		CertMap certMap = new CertMap();
		certMap.add(ClassPath.getInstance("../conf/localhost.cer"));

		Connector connector = new Connector();
		BroadcastServer broadcastServer = new BroadcastServer(2323);
//		ForwardServer broadcastServer = new ForwardServer(connector, 2323, "localhost", 8080);
		SSLFilterFactory sslffServer = new SSLFilterFactory(SSLUtil.
				getSSLContext(certKey, certMap), SSLFilter.SERVER_WANT_AUTH);
		broadcastServer.getFilterFactories().add(sslffServer);
		broadcastServer.getFilterFactories().add(new DumpFilterFactory());
		connector.add(broadcastServer);

		ForwardServer forwardServer = new ForwardServer(connector, 2424, "localhost", 2323);
		SSLFilterFactory sslffClient = new SSLFilterFactory(SSLUtil.
				getSSLContext(certKey, certMap), SSLFilter.CLIENT);
		forwardServer.getRemoteFilterFactories().add(sslffClient);
		forwardServer.getRemoteFilterFactories().add(new DumpFilterFactory().
				setDumpStream(System.err).setUseClientMode(true));
		connector.add(forwardServer);

		while (true) {
			while (connector.doEvents()) {/**/}
			Thread.sleep(16);
		}
	}
}