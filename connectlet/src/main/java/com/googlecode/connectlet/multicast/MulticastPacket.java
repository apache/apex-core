package com.googlecode.connectlet.multicast;

import com.googlecode.connectlet.Bytes;
import com.googlecode.connectlet.packet.PacketParser;
import com.googlecode.connectlet.packet.PacketException;

class MulticastPacket {
	// Edge Commands
	static final int EDGE_PING = 0x100;
	static final int EDGE_CONNECT = 0x101;
	static final int EDGE_DATA = 0x102;
	static final int EDGE_DISCONNECT = 0x103;

	// Origin Commands
	static final int ORIGIN_PONG = 0x200;
	static final int ORIGIN_MULTICAST = 0x201;
	static final int ORIGIN_DATA = 0x202;
	static final int ORIGIN_DISCONNECT = 0x203;
	static final int ORIGIN_CLOSE = 0x204;

	static final int HEAD_SIZE = 16;

	private static final int HEAD_TAG = 0x234c;

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

	int connId, command, numConns, size;

	/** @param len */
	MulticastPacket(byte[] b, int off, int len) {
		connId = Bytes.toInt(b, off + 4);
		command = Bytes.toShort(b, off + 8);
		numConns = Bytes.toShort(b, off + 10);
		size = Bytes.toShort(b, off + 12) & 0xffff;
	}

	MulticastPacket(int connId, int command, int numConns, int size) {
		this.connId = connId;
		this.command = command;
		this.numConns = numConns;
		this.size = size;
	}

	byte[] getHead() {
		byte[] head = new byte[16];
		Bytes.setShort(HEAD_TAG, head, 0);
		Bytes.setShort(16 + numConns * 4 + size, head, 2);
		Bytes.setInt(connId, head, 4);
		Bytes.setShort(command, head, 8);
		Bytes.setShort(numConns, head, 10);
		Bytes.setShort(size, head, 12);
		Bytes.setShort(0, head, 14);
		return head;
	}
}