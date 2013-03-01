package com.googlecode.connectlet.misc;

import com.googlecode.connectlet.FilterFactory;

/** A Factory to create {@link ZLibFilter}s. */
public class ZLibFilterFactory implements FilterFactory {
	@Override
	public ZLibFilter createFilter() {
		return new ZLibFilter();
	}
}