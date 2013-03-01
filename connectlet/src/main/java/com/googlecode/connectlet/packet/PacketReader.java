package com.googlecode.connectlet.packet;

import com.googlecode.connectlet.ByteArrayQueue;

/** A class to parse and consume packets. */ 
public class PacketReader {
	private int packetSize = 0;
	private ByteArrayQueue baq = new ByteArrayQueue();
	private PacketParser parser;
	private PacketHandler handler;

	/** Creates a PacketReader with a given parser and a given handler. */
	public PacketReader(PacketParser parser, PacketHandler handler) {
		this.parser = parser;
		this.handler = handler;
	}

	/**
	 * Parse packets when data arrived. This routine will consume every successfully
	 * parsed packet by calling {@link PacketHandler#onData(byte[], int, int)}.
	 */
	public void onData(byte[] b, int off, int len) throws PacketException {
		baq.add(b, off, len);
		while (packetSize > 0 || (packetSize = parser.getPacketSize(baq.array(),
				baq.offset(), baq.length())) > 0) {
			if (baq.length() < packetSize) {
				return;
			}
			handler.onData(baq.array(), baq.offset(), packetSize);
			baq.remove(packetSize);
			packetSize = 0;
		}
	}
}