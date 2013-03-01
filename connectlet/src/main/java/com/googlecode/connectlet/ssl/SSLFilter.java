package com.googlecode.connectlet.ssl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;

import com.googlecode.connectlet.ByteArrayQueue;
import com.googlecode.connectlet.Bytes;
import com.googlecode.connectlet.Filter;
import com.googlecode.connectlet.Connection.Event;

/** An SSL filter which makes a connection secure */
public class SSLFilter extends Filter implements Runnable {
	/**
	 * Indicates that SSLFilter is created in server mode with
	 * NO client authentication desired.
	 *
	 * @see #SSLFilter(ExecutorService, SSLContext, int)
	 * @see #SSLFilter(ExecutorService, SSLContext, int, String, int)
	 */
	public static final int SERVER_NO_AUTH = 0;
	/**
	 * Indicates that SSLFilter is created in server mode with
	 * client authentication REQUESTED.
	 *
	 * @see #SSLFilter(ExecutorService, SSLContext, int)
	 * @see #SSLFilter(ExecutorService, SSLContext, int, String, int)
	 */
	public static final int SERVER_WANT_AUTH = 1;
	/**
	 * Indicates that SSLFilter is created in server mode with
	 * client authentication REQUIRED.
	 *
	 * @see #SSLFilter(ExecutorService, SSLContext, int)
	 * @see #SSLFilter(ExecutorService, SSLContext, int, String, int)
	 */
	public static final int SERVER_NEED_AUTH = 2;
	/**
	 * Indicates that SSLFilter is created in client mode.
	 *
	 * @see #SSLFilter(ExecutorService, SSLContext, int)
	 * @see #SSLFilter(ExecutorService, SSLContext, int, String, int)
	 */
	public static final int CLIENT = 3;

	private static final int EVENT_TASK = 2246;

	private ExecutorService executor;
	private SSLEngine ssle;
	private int appBBSize;
	private byte[] requestBytes;
	private ByteBuffer requestBB;
	private HandshakeStatus hs = HandshakeStatus.NEED_UNWRAP;
	private ByteArrayQueue baqRecv = new ByteArrayQueue();
	private ByteArrayQueue baqToSend = new ByteArrayQueue();

	/**
	 * Creates an SSLFilter with the given {@link ExecutorService},
	 * {@link SSLContext} and mode
	 * @param mode - SSL mode, must be {@link #SERVER_NO_AUTH},
	 *        {@link #SERVER_WANT_AUTH}, {@link #SERVER_NEED_AUTH} or {@link #CLIENT}.
	 */
	public SSLFilter(ExecutorService executor, SSLContext sslc, int mode) {
		this(executor, sslc, mode, null, 0);
	}

	/**
	 * Creates an SSLFilter with the given {@link ExecutorService},
	 * {@link SSLContext}, mode and advisory peer information
	 * @param mode - SSL mode, must be {@link #SERVER_NO_AUTH},
	 *        {@link #SERVER_WANT_AUTH}, {@link #SERVER_NEED_AUTH} or {@link #CLIENT}.
	 * @param peerHost - Advisory peer information.
	 * @param peerPort - Advisory peer information.
	 */
	public SSLFilter(ExecutorService executor, SSLContext sslc, int mode,
			String peerHost, int peerPort) {
		this.executor = executor;
		if (peerHost == null) {
			ssle = sslc.createSSLEngine();
		} else {
			ssle = sslc.createSSLEngine(peerHost, peerPort);
		}
		ssle.setUseClientMode(mode == CLIENT);
		if (mode == SERVER_NEED_AUTH) {
			ssle.setNeedClientAuth(true);
		}
		if (mode == SERVER_WANT_AUTH) {
			ssle.setWantClientAuth(true);
		}
		appBBSize = ssle.getSession().getApplicationBufferSize();
		requestBytes = new byte[appBBSize];
		requestBB = ByteBuffer.wrap(requestBytes);
	}

	@Override
	public void run() {
		Runnable task;
		while ((task = ssle.getDelegatedTask()) != null) {
			task.run();
		}
		getConnection().dispatchEvent(EVENT_TASK, true);
	}

	@Override
	protected void send(byte[] b, int off, int len) {
		if (hs != HandshakeStatus.FINISHED) {
			baqToSend.add(b, off, len);
			return;
		}
		try {
			wrap(b, off, len);
		} catch (IOException e) {
			getConnection().disconnect();
		}
	}

	@Override
	protected void onRecv(byte[] b, int off, int len) {
		baqRecv.add(b, off, len);
		if (hs != HandshakeStatus.FINISHED) {
			if (hs != HandshakeStatus.NEED_TASK) {
				try {
					doHandshake();
				} catch (IOException e) {
					getConnection().disconnect();
				}
			}
			return;
		}
		SSLEngineResult result;
		do {
			try {
				result = unwrap();
			} catch (Exception e) {
				getConnection().disconnect();
				return;
			}
			switch (result.getStatus()) {
			case OK:
			case BUFFER_UNDERFLOW: // Wait for next onRecv
				break;
			case BUFFER_OVERFLOW:
				appBBSize = ssle.getSession().getApplicationBufferSize();
				break;
			default:
				getConnection().disconnect();
				return;
			}
		} while (result.getStatus() != Status.BUFFER_UNDERFLOW);
		recv();
	}

	@Override
	protected void onEvent(Event event) {
		if (event.getType() == EVENT_TASK) {
			hs = ssle.getHandshakeStatus();
			try {
				doHandshake();
			} catch (IOException e) {
				getConnection().disconnect();
			}
		}
		super.onEvent(event);
	}

	@Override
	protected void onConnect() {
		try {
			doHandshake();
		} catch (IOException e) {
			getConnection().disconnect();
		}
	}

	private SSLEngineResult unwrap() throws IOException {
		if (requestBB.remaining() < appBBSize) {
			byte[] newRequestBytes = new byte[requestBytes.length * 2];
			ByteBuffer bb = ByteBuffer.wrap(newRequestBytes);
			requestBB.flip();
			bb.put(requestBB);
			requestBytes = newRequestBytes;
			requestBB = bb;
		}
		ByteBuffer inNetBB = ByteBuffer.wrap(baqRecv.array(),
				baqRecv.offset(), baqRecv.length());
		int pos = inNetBB.position();
		SSLEngineResult result = ssle.unwrap(inNetBB, requestBB);
		baqRecv.remove(inNetBB.position() - pos);
		return result;
	}

	private SSLEngineResult wrap(byte[] b, int off, int len) throws IOException {
		ByteBuffer srcBB = ByteBuffer.wrap(b, off, len);
		byte[] outNetBytes = null;
		SSLEngineResult result;
		do {
			int packetBBSize = ssle.getSession().getPacketBufferSize();
			if (outNetBytes == null || outNetBytes.length < packetBBSize) {
				outNetBytes = new byte[packetBBSize];
			}
			ByteBuffer outNetBB = ByteBuffer.wrap(outNetBytes);
			result = ssle.wrap(srcBB, outNetBB);
			if (result.getStatus() != Status.OK) {
				throw new IOException();
			}
			super.send(outNetBytes, 0, outNetBB.position());
		} while (srcBB.remaining() > 0);
		return result;
	}

	private void doHandshake() throws IOException {
		while (hs != HandshakeStatus.FINISHED) {
			switch (hs) {
			case NEED_UNWRAP:
				SSLEngineResult result = unwrap();
				hs = result.getHandshakeStatus();
				switch (result.getStatus()) {
				case OK:
					if (hs == HandshakeStatus.NOT_HANDSHAKING) {
						throw new IOException();
					}
					if (hs == HandshakeStatus.NEED_TASK) {
						executor.execute(this);
						return;
					}
					break;
				case BUFFER_UNDERFLOW: // Wait for next onRecv
					if (hs == HandshakeStatus.NEED_UNWRAP) {
						return;
					}
					break;
				case BUFFER_OVERFLOW:
					appBBSize = ssle.getSession().getApplicationBufferSize();
					break;
				default:
					throw new IOException();
				}
				break;
			case NEED_WRAP:
				result = wrap(Bytes.EMPTY_BYTES, 0, 0);
				hs = result.getHandshakeStatus();
				if (hs == HandshakeStatus.NEED_TASK) {
					executor.execute(this);
					return;
				}
				break;
			case FINISHED:
				break;
			default:
				throw new IOException();
			}
		}
		// hs == HandshakeStatus.FINISHED
		super.onConnect();
		recv();
		if (baqRecv.length() > 0) {
			onRecv(baqRecv.array(), baqRecv.offset(), baqRecv.length());
		}
		if (baqToSend.length() > 0) {
			send(baqToSend.array(), baqToSend.offset(), baqToSend.length());
		}
		baqToSend = null;
	}

	private void recv() {
		if (requestBB.position() > 0) {
			super.onRecv(requestBytes, 0, requestBB.position());
			requestBB.clear();
		}
	}

	/** @return The SSLSession for this connection. */
	public SSLSession getSession() {
		return ssle.getSession();
	}
}