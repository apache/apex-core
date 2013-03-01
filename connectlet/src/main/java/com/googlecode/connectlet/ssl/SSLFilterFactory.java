package com.googlecode.connectlet.ssl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.SSLContext;

import com.googlecode.connectlet.FilterFactory;

public class SSLFilterFactory implements FilterFactory, AutoCloseable {
	private ExecutorService executor = Executors.newCachedThreadPool();
	private SSLContext sslc;
	private int mode;

	public SSLFilterFactory(SSLContext sslc, int mode) {
		this.sslc = sslc;
		this.mode = mode;
	}

	@Override
	public SSLFilter createFilter() {
		return new SSLFilter(executor, sslc, mode);
	}

	@Override
	public void close() {
		executor.shutdown();
	}
}