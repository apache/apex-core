package com.googlecode.connectlet;

import java.util.LinkedHashSet;
import java.util.Random;

public class TestClient {
	static int connections = 0;
	static int responses = 0;
	static int errors = 0;

	private static Connector newConnector() {
		return new Connector();
	}

	// To test under Windows, you should add a registry item "MaxUserPort = 65534" in
	// HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters
	public static void main(String[] args) throws Exception {
		final LinkedHashSet<Connection> connectionSet = new LinkedHashSet<Connection>();
		Random random = new Random();
		// Evade resource leak warning
		Connector connector = newConnector();
		int count = 0;
		long startTime = System.currentTimeMillis();
		byte[] data = new byte[] {'-'};
		while (true) {
			for (Connection conn : connectionSet.toArray(new Connection[0])) {
				// "conn.onDisconnect()" might change "connectionSet"
				if (random.nextDouble() < .01) {
					conn.send(data);
				}
			}
			if (!connector.doEvents()) {
				Thread.sleep(16);
			}
			count ++;
			if (count == 100) {
				count = 0;
				System.out.print("Time: " +
						(System.currentTimeMillis() - startTime) + ", ");
				System.out.print("Connections: " + connections + ", ");
				System.out.print("Errors: " + errors + ", ");
				System.out.println("Responses: " + responses);
			}
			connector.connect(new Connection() {
				@Override
				protected void onRecv(byte[] b, int off, int len) {
					responses ++;
				}

				@Override
				protected void onConnect() {
					connectionSet.add(this);
					connections ++;
				}

				@Override
				protected void onDisconnect() {
					connectionSet.remove(this);
					errors ++;
				}
			}, "localhost", 2626);
		}
	}
}