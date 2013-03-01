package com.googlecode.connectlet.multicast;

import java.util.Iterator;

import com.googlecode.connectlet.Connection;
import com.googlecode.connectlet.Connector;
import com.googlecode.connectlet.misc.DumpFilterFactory;
import com.googlecode.connectlet.misc.ZLibFilterFactory;

public class TestMulticast {
	private static Connector newConnector() {
		return new Connector();
	}

	public static void main(String[] args) throws Exception {
		// Evade resource leak warning
		Connector connector = newConnector();
		connector.add(new OriginServer(2323) {
			{
				getVirtualFilterFactories().add(new DumpFilterFactory().setDumpText(true));
				getVirtualFilterFactories().add(new ZLibFilterFactory());
			}

			Connection multicast = createMulticast(new
					Iterable<Connection>() {
				@Override
				public Iterator<Connection> iterator() {
					return getVirtualConnectionSet().iterator();
				}
			});

			@Override
			protected Connection createVirtualConnection() {
				return new Connection() {
					@Override
					protected void onRecv(byte[] b, int off, int len) {
						multicast.send(b, off, len);
					}
				};
			}
		});
		EdgeServer edge = new EdgeServer(2424);
		connector.add(edge);
		connector.connect(edge.getOriginConnection(), "localhost", 2323);
		edge = new EdgeServer(2525);
		connector.add(edge);
		connector.connect(edge.getOriginConnection(), "localhost", 2323);
		while (true) {
			while (connector.doEvents()) {/**/}
			Thread.sleep(16);
		}
	}
}