package com.googlecode.connectlet.misc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import com.googlecode.connectlet.Connection;
import com.googlecode.connectlet.Connector;
import com.googlecode.connectlet.FilterFactory;
import com.googlecode.connectlet.ServerConnection;

class PeerConnection extends Connection {
	PeerConnection peer;

	PeerConnection(PeerConnection peer) {
		this.peer = peer;
	}

	@Override
	protected void onRecv(byte[] b, int off, int len) {
		peer.send(b, off, len);
	}

	@Override
	protected void onDisconnect() {
		if (peer != null) {
			peer.peer = null;
			peer.disconnect();
		}
	}
}

class ForwardConnection extends PeerConnection {
	private ForwardServer forward;

	ForwardConnection(ForwardServer forward) {
		super(null);
		this.forward = forward;
	}

	@Override
	protected void onConnect() {
		peer = new PeerConnection(this);
		peer.appendFilters(forward.getRemoteFilterFactories());
		try {
			forward.connector.connect(peer, forward.remote);
		} catch (IOException e) {
			peer = null;
			disconnect();
		}
	}
}

/** A port redirecting server. */
public class ForwardServer extends ServerConnection {
	Connector connector;
	InetSocketAddress remote;

	@Override
	protected Connection createConnection() {
		return new ForwardConnection(this);
	}

	/**
	 * Creates a ForwardServer.
	 * @param connector - A connector which remote connections registered to.
	 * @param port - The listening port to redirect.
	 * @param remoteHost - The remote (redirected) host.
	 * @param remotePort - The remote (redirected) port.
	 * @throws IOException If an I/O error occurs when opening the port.
	 */
	public ForwardServer(Connector connector,
			int port, String remoteHost, int remotePort) throws IOException {
		super(port);
		this.connector = connector;
		remote = new InetSocketAddress(remoteHost, remotePort);
	}

	/**
	 * Creates a ForwardServer.
	 * @param connector - A connector which remote connections registered to.
	 * @param local - The binding / listening address to redirect.
	 * @param remote - The remote (redirected) address.
	 * @throws IOException If an I/O error occurs when opening the port.
	 */
	public ForwardServer(Connector connector,
			InetSocketAddress local, InetSocketAddress remote) throws IOException {
		super(local);
		this.connector = connector;
		this.remote = remote;
	}

	private ArrayList<FilterFactory> remoteFilterFactories = new ArrayList<FilterFactory>();

	/** @return A list of {@link FilterFactory}s applied to remote connections. */
	public ArrayList<FilterFactory> getRemoteFilterFactories() {
		return remoteFilterFactories;
	}
}