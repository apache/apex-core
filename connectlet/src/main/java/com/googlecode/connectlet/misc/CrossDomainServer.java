package com.googlecode.connectlet.misc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.googlecode.connectlet.ByteArrayQueue;
import com.googlecode.connectlet.Bytes;
import com.googlecode.connectlet.Connection;
import com.googlecode.connectlet.Connector;
import com.googlecode.connectlet.ServerConnection;
import com.googlecode.connectlet.Connector.Listener;

/** A {@link ServerConnection} which provides cross-domain policy service for Adobe Flash. */
public class CrossDomainServer extends ServerConnection implements Listener {
	private File policyFile;
	private long lastAccessed = 0;

	/**
	 * Creates a CrossDomainServer with a given policy file and
	 * the default listening port 843.
	 */
	public CrossDomainServer(File policyFile) throws IOException {
		this(policyFile, 843);
	}

	/** Creates a CrossDomainServer with a given policy file and a given listning port. */
	public CrossDomainServer(File policyFile, int port) throws IOException {
		super(port);
		this.policyFile = policyFile;
	}

	@Override
	public void onEvent() {
		long now = System.currentTimeMillis();
		if (now > lastAccessed + 60000) {
			lastAccessed = now;
			try {
        FileInputStream fin = new FileInputStream(policyFile);
				ByteArrayQueue baq = new ByteArrayQueue();
				byte[] buffer = new byte[Connector.MAX_BUFFER_SIZE];
				int bytesRead;
				while ((bytesRead = fin.read(buffer)) > 0) {
					baq.add(buffer, 0, bytesRead);
				}
				policyBytes = new byte[baq.length() + 1];
				baq.remove(policyBytes, 0, policyBytes.length - 1);
				policyBytes[policyBytes.length - 1] = 0;
        fin.close();
			}
      catch (Exception e) {/**/}
		}
	}

	byte[] policyBytes = Bytes.EMPTY_BYTES;

	@Override
	protected Connection createConnection() {
		return new Connection() {
			@Override
			protected void onRecv(byte[] b, int off, int len) {
				if (b[len - 1] == 0) {
					send(policyBytes);
					disconnect();
				}
			}
		};
	}
}