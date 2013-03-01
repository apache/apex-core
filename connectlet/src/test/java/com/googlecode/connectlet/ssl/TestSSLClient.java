package com.googlecode.connectlet.ssl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.SSLContext;

import com.googlecode.connectlet.Bytes;
import com.googlecode.connectlet.Connection;
import com.googlecode.connectlet.Connector;

public class TestSSLClient {
	static boolean connected = false;

	private static void closeConnector(Connector connector) {
		connector.close();
	}

	public static void main(String[] args) throws Exception {
		ExecutorService executor = Executors.newCachedThreadPool();
		final Connector connector = new Connector();
		SSLContext sslc = SSLUtil.getSSLContext(null, null);
		final SSLFilter sslf1 = new SSLFilter(executor, sslc,
				SSLFilter.CLIENT, "localhost", 2323);
		final SSLFilter sslf2 = new SSLFilter(executor, sslc,
				SSLFilter.CLIENT, "localhost", 2323);
		final Connection connection1 = new Connection() {
			@Override
			protected void onConnect() {
				System.out.println(Bytes.toHexLower(sslf1.getSession().getId()));
				connected = true;
			}

			@Override
			protected void onRecv(byte[] b, int off, int len) {/**/}
		};
		connection1.appendFilter(sslf1);
		Connection connection2 = new Connection() {
			@Override
			protected void onConnect() {
				System.out.println(Bytes.toHexLower(sslf2.getSession().getId()));
				try {
					connector.connect(connection1, "localhost", 2323);
				} catch (Exception e) {/**/}
			}

			@Override
			protected void onRecv(byte[] b, int off, int len) {/**/}
		};
		connection2.appendFilter(sslf2);
		connector.connect(connection2, "localhost", 2323);
		while (!connected) {
			while (connector.doEvents()) {/**/}
			Thread.sleep(16);
		}
		// Evade resource leak warning
		closeConnector(connector);
		executor.shutdown();
	}
}