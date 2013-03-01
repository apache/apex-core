package com.googlecode.connectlet.packet;

/** This interface provides the way to parse packets. */
public interface PacketParser {
	/**
	 * @return Packet size according to the beginning bytes of a packet,
	 *         should be zero if not enough bytes to determine the packet size.
	 * @throws PacketException if data cannot be parsed into a packet.
	 */
	public int getPacketSize(byte[] b, int off, int len) throws PacketException;
}