package com.googlecode.connectlet.multicast;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;

import com.googlecode.connectlet.Bytes;
import com.googlecode.connectlet.Connection;
import com.googlecode.connectlet.ServerConnection;
import com.googlecode.connectlet.Connector.Listener;
import com.googlecode.connectlet.packet.PacketFilter;

class IdPool {
	private int maxId = 0;

	private ArrayDeque<Integer> returned = new ArrayDeque<Integer>();
	private HashSet<Integer> borrowed = new HashSet<Integer>();

	public int borrowId() {
		Integer i = returned.poll();
		if (i == null) {
			i = Integer.valueOf(maxId);
			maxId ++;
		}
		borrowed.add(i);
		return i.intValue();
	}

	public void returnId(int id) {
		Integer i = Integer.valueOf(id);
		if (borrowed.remove(i)) {
			returned.add(i);
		}
	}
}

class ClientConnection extends Connection {
	private OriginConnection origin;
	private int connId;

	ClientConnection(OriginConnection origin) {
		this.origin = origin;
	}

	@Override
	protected void onRecv(byte[] b, int off, int len) {
		origin.send(new MulticastPacket(connId,
				MulticastPacket.EDGE_DATA, 0, len).getHead());
		origin.send(b, off, len);
	}

	@Override
	protected void onConnect() {
		connId = origin.idPool.borrowId();
		origin.send(new MulticastPacket(connId,
				MulticastPacket.EDGE_CONNECT, 0, 16).getHead());
		byte[] localAddrBytes, remoteAddrBytes;
		try {
			localAddrBytes = InetAddress.getByName(getLocalAddr()).getAddress();
			remoteAddrBytes = InetAddress.getByName(getRemoteAddr()).getAddress();
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
		byte[] addrBytes = new byte[16];
		System.arraycopy(localAddrBytes, 0, addrBytes, 0, 4);
		Bytes.setShort(getLocalPort(), addrBytes, 4);
		Bytes.setShort(0, addrBytes, 6);
		System.arraycopy(remoteAddrBytes, 0, addrBytes, 8, 4);
		Bytes.setShort(getRemotePort(), addrBytes, 12);
		Bytes.setShort(0, addrBytes, 14);
		origin.send(addrBytes);
		origin.connMap.put(Integer.valueOf(connId), this);
	}

	boolean activeClose = false;

	@Override
	protected void onDisconnect() {
		if (activeClose) {
			return;
		}
		origin.connMap.remove(Integer.valueOf(connId));
		// Do not return connId until ORIGIN_CLOSE received
		// origin.idPool.returnId(connId);
		origin.send(new MulticastPacket(connId,
				MulticastPacket.EDGE_DISCONNECT, 0, 0).getHead());
	}
}

class OriginConnection extends Connection {
	HashMap<Integer, ClientConnection> connMap = new HashMap<Integer, ClientConnection>();
	IdPool idPool = new IdPool();

	{
		appendFilter(new PacketFilter(MulticastPacket.getParser()));
	}

	@Override
	protected void onRecv(byte[] b, int off, int len) {
		MulticastPacket packet = new MulticastPacket(b, off, len);
		int command = packet.command;
		if (command == MulticastPacket.ORIGIN_MULTICAST) {
			int numConns = packet.numConns;
			if (16 + numConns * 4 + packet.size > len) {
				// throw new PacketException("Wrong Packet Size");
				disconnect();
				return;
			}
			for (int i = 0; i < numConns; i ++) {
				int connId = Bytes.toInt(b, off + 16 + i * 4);
				ClientConnection conn = connMap.get(Integer.valueOf(connId));
				if (conn != null) {
					conn.send(b, off + 16 + numConns * 4, packet.size);
				}
			}
		} else {
			int connId = packet.connId;
			if (command == MulticastPacket.ORIGIN_CLOSE) {
				idPool.returnId(connId);
			}
			ClientConnection conn = connMap.get(Integer.valueOf(connId));
			if (conn == null) {
				return;
			}
			if (command == MulticastPacket.ORIGIN_DATA) {
				if (16 + packet.size > len) {
					// throw new PacketException("Wrong Packet Size");
					disconnect();
					return;
				}
				conn.send(b, off + 16, packet.size);
			} else {
				connMap.remove(Integer.valueOf(connId));
				idPool.returnId(connId);
				conn.activeClose = true;
				conn.disconnect();
			}
		}
	}

	@Override
	protected void onDisconnect() {
		for (ClientConnection conn : connMap.values().
				toArray(new ClientConnection[0])) {
			// "conn.onDisconnect()" might change "connMap"
			conn.activeClose = true;
			conn.disconnect();
		}
	}
}

/**
 * An edge server for the {@link OriginServer}. All the accepted connections
 * will become the virtual connections of the connected OriginServer.
 * @see OriginServer
 */
public class EdgeServer extends ServerConnection implements Listener {
	@Override
	protected Connection createConnection() {
		return new ClientConnection(origin);
	}

	private long lastAccessed = System.currentTimeMillis();
	private OriginConnection origin = new OriginConnection();

	/**
	 * Creates an EdgeServer with a given port, which accepts client connections.<p>
	 * The edge connection must be connected to the OriginServer immediately,
	 * after the EdgeServer created.
	 * Here is the code to make an edge server working:<p><code>
	 * Connector connector = new Connector();<br>
	 * EdgeServer edge = new EdgeServer(2424);<br>
	 * connector.add(edge);<br>
	 * connector.connect(edge.getOriginConnection(), "localhost", 2323);<br>
	 * while (edge.getOriginConnection().isOpen()) {<br>
	 * &nbsp;&nbsp;while (connector.doEvents()) {}<br>
	 * &nbsp;&nbsp;Thread.sleep(16);<br>
	 * }<br>
	 * connector.close();</code>
	 * @see #getOriginConnection()
	 */
	public EdgeServer(int port) throws IOException {
		super(port);
	}

	/** @return The connection to the {@link OriginServer}. */
	public Connection getOriginConnection() {
		return origin;
	}

	@Override
	public void onEvent() {
		long now = System.currentTimeMillis();
		if (now > lastAccessed + 60000) {
			origin.send(new MulticastPacket(0,
					MulticastPacket.EDGE_PING, 0, 0).getHead());
			lastAccessed = now;
		}
	}
}