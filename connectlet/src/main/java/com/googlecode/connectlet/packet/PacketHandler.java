package com.googlecode.connectlet.packet;

/** This interface provides the way to consume packets. */
public interface PacketHandler {
	/** Consumes a packet. */
	public void onData(byte[] b, int off, int len);
}