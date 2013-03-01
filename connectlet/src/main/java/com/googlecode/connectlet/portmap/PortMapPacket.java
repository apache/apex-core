package com.googlecode.connectlet.portmap;

import com.googlecode.connectlet.Bytes;
import com.googlecode.connectlet.packet.PacketParser;
import com.googlecode.connectlet.packet.PacketException;

class PortMapPacket {
	// Client Commands
	static final int CLIENT_PING = 0x100;
	static final int CLIENT_OPEN = 0x101;
	static final int CLIENT_DATA = 0x102;
	static final int CLIENT_DISCONNECT = 0x103;
	static final int CLIENT_CLOSE = 0x104;

	// Server Commands
	static final int SERVER_PONG = 0x200;
	static final int SERVER_CONNECT = 0x201;
	static final int SERVER_DATA = 0x202;
	static final int SERVER_DISCONNECT = 0x203;

	static final int HEAD_SIZE = 16;

	private static final int HEAD_TAG = 0x234d;

	private static PacketParser parser = new PacketParser() {
		@Override
		public int getPacketSize(byte[] b, int off, int len) throws PacketException {
			if (len < HEAD_SIZE) {
				return 0;
			}
			if (Bytes.toShort(b, off) != HEAD_TAG) {
				throw new PacketException("Wrong Packet Head");
			}
			int packetSize = Bytes.toShort(b, off + 2) & 0xffff;
			if (packetSize < HEAD_SIZE) {
				throw new PacketException("Wrong Packet Size");
			}
			return packetSize;
		}
	};

	static PacketParser getParser() {
		return parser;
	}

	int connId, command, port, size;

	/** @param len */
	PortMapPacket(byte[] b, int off, int len) {
		connId = Bytes.toInt(b, off + 4);
		command = Bytes.toShort(b, off + 8);
		port = Bytes.toShort(b, off + 10) & 0xffff;
		size = Bytes.toShort(b, off + 12) & 0xffff;
	}

	PortMapPacket(int connId, int command, int port, int size) {
		this.connId = connId;
		this.command = command;
		this.port = port;
		this.size = size;
	}

	byte[] getHead() {
		byte[] head = new byte[16];
		Bytes.setShort(HEAD_TAG, head, 0);
		Bytes.setShort(size + 16, head, 2);
		Bytes.setInt(connId, head, 4);
		Bytes.setShort(command, head, 8);
		Bytes.setShort(port, head, 10);
		Bytes.setShort(size, head, 12);
		Bytes.setShort(0, head, 14);
		return head;
	}
}