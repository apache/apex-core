package com.googlecode.connectlet;

/**
 * A Factory to create {@link Filter}s,
 * applied to {@link Connector} and {@link ServerConnection}.
 */
public interface FilterFactory {
	/** @return A new <b>Filter</b>. */
	public Filter createFilter();
}