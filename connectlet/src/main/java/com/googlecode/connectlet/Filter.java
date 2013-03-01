package com.googlecode.connectlet;

import com.googlecode.connectlet.Connection.Event;

/**
 * Provides a filtering task on a {@link Connection}.<p>
 * 
 * A <b>Filter</b> can filter sent data from the application side to the network side,
 * or filter received data and events (including connecting and disconnecting events)
 * from the network side to the application side.
 */
public class Filter {
	Connection connection;
	Filter netFilter, appFilter;

	/** @return The {@link Connection} which the <b>Filter</b> applies to. */
	protected Connection getConnection() {
		return connection;
	}

	/** Filters sent data from the application side to the network side */
	protected void send(byte[] b, int off, int len) {
		netFilter.send(b, off, len);
	}

	/** Filters received data from the network side to the application side */
	protected void onRecv(byte[] b, int off, int len) {
		appFilter.onRecv(b, off, len);
	}

	/** Filters events from the network side to the application side */
	protected void onEvent(Event event) {
		appFilter.onEvent(event);
	}

	/** Filters connecting events from the network side to the application side */
	protected void onConnect() {
		appFilter.onConnect();
	}

	/** Filters disconnecting events from the network side to the application side */
	protected void onDisconnect() {
		appFilter.onDisconnect();
	}
}