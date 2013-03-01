package com.googlecode.connectlet.misc;

import java.io.IOException;

import com.googlecode.connectlet.Connection;
import com.googlecode.connectlet.ServerConnection;

/**
 * A {@link ServerConnection} which sends received data
 * from one {@link Connection} to all its accepted connections.<p>
 *
 * Note that all its accepted connections will be closed automatically
 * when it is removed from a connector.
 */
public class BroadcastServer extends ServerConnection {
	ConnectionSet connections = new ConnectionSet();
	boolean noEcho;

	/** Creates a BroadcastServer with a given listening port. */
	public BroadcastServer(int port) throws IOException {
		this(port, false);
	}

	/**
	 * Creates a BroadcastServer with a given listening port.
	 *
	 * @param noEcho - <code>true</code> if received data is not allowed to
	 *        send back to the original connection.
	 */
	public BroadcastServer(int port, boolean noEcho) throws IOException {
		super(port);
		this.noEcho = noEcho;
		getFilterFactories().add(connections);
	}

	@Override
	protected Connection createConnection() {
		return new Connection() {
			@Override
			protected void onRecv(byte[] b, int off, int len) {
				for (Connection connection : connections.toArray(new Connection[0])) {
					// "connection.onDisconnect()" might change "connections"
					if (!noEcho || connection != this) {
						connection.send(b, off, len);
					}
				}
			}
		};
	}
}