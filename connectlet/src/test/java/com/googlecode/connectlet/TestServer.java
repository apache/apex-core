package com.googlecode.connectlet;

public class TestServer {
	static int accepts = 0;
	static int requests = 0;
	static int errors = 0;

	private static Connector newConnector() {
		return new Connector();
	}

	public static void main(String[] args) throws Exception {
		// Evade resource leak warning
		Connector connector = newConnector();
		connector.add(new ServerConnection(2626) {
			@Override
			protected Connection createConnection() {
				return new Connection() {
					@Override
					protected void onRecv(byte[] b, int off, int len) {
						send(b, off, len);
						requests ++;
					}

					@Override
					protected void onConnect() {
						accepts ++;
					}

					@Override
					protected void onDisconnect() {
						errors ++;
					}
				};
			}
		});
		long startTime = System.currentTimeMillis();
		int count = 0;
		while (true) {
			if (!connector.doEvents()) {
				Thread.sleep(16);
			}
			count ++;
			if (count == 100) {
				count = 0;
				System.out.print("Time: " +
						(System.currentTimeMillis() - startTime) + ", ");
				System.out.print("Accepts: " + accepts + ", ");
				System.out.print("Errors: " + errors + ", ");
				System.out.println("Requests: " + requests);
			}
		}
	}
}