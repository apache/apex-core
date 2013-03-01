package com.googlecode.connectlet.misc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;

import com.googlecode.connectlet.Connection;
import com.googlecode.connectlet.Connector;
import com.googlecode.connectlet.FilterFactory;
import com.googlecode.connectlet.ServerConnection;
import com.googlecode.connectlet.Connection.Writer;
import com.googlecode.connectlet.util.ClassPath;

public class TestFlash {
	private static Connector newConnector() {
		return new Connector();
	}

	public static void main(String[] args) throws Exception {
		// Evade resource leak warning
		Connector connector = newConnector();
		try {
			connector.add(new CrossDomainServer(ClassPath.
					getInstance("../conf/crossdomain.xml")));
		} catch (Exception e) {/**/}

		final LinkedHashSet<Connection> connections = new LinkedHashSet<Connection>();
		final Connection broadcast = new Connection();
		broadcast.setWriter(new Writer() {
			@Override
			public int write(byte[] b, int off, int len) throws IOException {
				for (Connection connection : connections.toArray(new Connection[0])) {
					// "connection.onDisconnect()" might change "connections"
					connection.getNetWriter().write(b, off, len);
				}
				return len;
			}
		});

		ServerConnection broadcastServer = new ServerConnection(23) {
			@Override
			protected Connection createConnection() {
				return new Connection() {
					@Override
					protected void onRecv(byte[] b, int off, int len) {
						broadcast.send(b, off, len);
					}

					@Override
					protected void onConnect() {
						connections.add(this);
					}

					@Override
					protected void onDisconnect() {
						connections.remove(this);
					}
				};
			}
		};
		connector.add(broadcastServer);

		ArrayList<FilterFactory> ffs = broadcastServer.getFilterFactories();
		// Application Data Dumped onto System.out
		ffs.add(new DumpFilterFactory().setDumpText(true));
		ffs.add(new ZLibFilterFactory());
		// Network Data Dumped onto System.err
		ffs.add(new DumpFilterFactory().setDumpStream(System.err));
		broadcast.appendFilters(ffs);

		DoSFilterFactory dosff = new DoSFilterFactory(60000, 65536, 60, 10);
		connector.getFilterFactories().add(dosff);
		connector.getFilterFactories().add(new IPTrustSet("127.0.0.1"));
		while (true) {
			while (connector.doEvents()) {/**/}
			dosff.onEvent();
			Thread.sleep(16);
		}
	}
}