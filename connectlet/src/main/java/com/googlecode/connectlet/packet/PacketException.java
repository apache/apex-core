package com.googlecode.connectlet.packet;

/** Signals that a packet exception of some sort has occurred. */
public class PacketException extends Exception {
	private static final long serialVersionUID = 1L;

	/** Creates a PacketException with null as its error detail message */
	public PacketException() {/**/}

	/**
	 * Creates a PacketException with the specified detail message.
	 *
	 * @param message - The detail message (which is saved for later retrieval
	 *        by the {@link Throwable#getMessage()} method).
	 */
	public PacketException(String message) {
		super(message);
	}
}