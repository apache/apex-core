package com.googlecode.connectlet.tools;

import java.awt.EventQueue;

import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JTextField;
import javax.swing.UIManager;

import com.googlecode.connectlet.portmap.PortMapClient;

public class PortMapClientFrame extends ConnectorFrame {
	private static final long serialVersionUID = 1L;

	private JTextField txtPrivateHost = new JTextField("localhost");
	private JTextField txtPrivatePort = new JTextField("8080");
	private JTextField txtMappingHost = new JTextField("localhost");
	private JTextField txtMappingPort = new JTextField("8341");
	private JTextField txtPublicPort = new JTextField("80");

	private PortMapClient client = null;

	void stop() {
		trayIcon.setToolTip(getTitle());
		startMenuItem.setLabel("Start");
		startButton.setText("Start");
		txtPrivateHost.setEnabled(true);
		txtPrivatePort.setEnabled(true);
		txtMappingHost.setEnabled(true);
		txtMappingPort.setEnabled(true);
		txtPublicPort.setEnabled(true);
	}

	@Override
	protected void start() {
		if (client != null) {
			client.disconnect();
			client = null;
			stop();
			return;
		}
		trayIcon.setToolTip(String.format(getTitle() + " (%s %s-%s)",
				txtMappingHost.getText(), txtPrivateHost.getText(),
				txtPrivatePort.getText()));
		startMenuItem.setLabel("Stop");
		startButton.setText("Stop");
		txtPrivateHost.setEnabled(false);
		txtPrivatePort.setEnabled(false);
		txtMappingHost.setEnabled(false);
		txtMappingPort.setEnabled(false);
		txtPublicPort.setEnabled(false);

		try {
			client = new PortMapClient(connector,
					Integer.parseInt(txtPublicPort.getText()), txtPrivateHost.getText(),
					Integer.parseInt(txtPrivatePort.getText()));
			connector.connect(client, txtMappingHost.getText(),
					Integer.parseInt(txtMappingPort.getText()));
		} catch (Exception e) {
			client = null;
			stop();
			JOptionPane.showMessageDialog(this, e.getMessage(),
					getTitle(), JOptionPane.WARNING_MESSAGE);
		}
	}

	@Override
	protected void doEvents() {
		super.doEvents();
		if (client == null || client.isOpen()) {
			return;
		}
		client = null;
		stop();
		EventQueue.invokeLater(new Runnable() {
			@Override
			public void run() {
				JOptionPane.showMessageDialog(PortMapClientFrame.this,
						"Mapping Connection Failed", getTitle(),
						JOptionPane.WARNING_MESSAGE);
			}
		});
	}

	public PortMapClientFrame() {
		super("Port Mapping Client", "Terminal", 288, 144, true);

		JLabel lblPrivateHost = new JLabel("Private Host");
		lblPrivateHost.setBounds(6, 6, 96, 24);
		add(lblPrivateHost);

		txtPrivateHost.setBounds(102, 6, 96, 24);
		txtPrivateHost.enableInputMethods(false);
		add(txtPrivateHost);

		JLabel lblPrivatePort = new JLabel("Port");
		lblPrivatePort.setBounds(204, 6, 42, 24);
		add(lblPrivatePort);

		txtPrivatePort.setBounds(240, 6, 42, 24);
		txtPrivatePort.enableInputMethods(false);
		add(txtPrivatePort);

		JLabel lblMappingHost = new JLabel("Mapping Host");
		lblMappingHost.setBounds(6, 36, 96, 24);
		add(lblMappingHost);

		txtMappingHost.setBounds(102, 36, 96, 24);
		txtMappingHost.enableInputMethods(false);
		add(txtMappingHost);

		JLabel lblMappingPort = new JLabel("Port");
		lblMappingPort.setBounds(204, 36, 42, 24);
		add(lblMappingPort);

		txtMappingPort.setBounds(240, 36, 42, 24);
		txtMappingPort.enableInputMethods(false);
		add(txtMappingPort);

		JLabel lblPublicHost = new JLabel("Public");
		lblPublicHost.setBounds(6, 66, 96, 24);
		add(lblPublicHost);

		JLabel lblPublicPort = new JLabel("Port");
		lblPublicPort.setBounds(204, 66, 42, 24);
		add(lblPublicPort);

		txtPublicPort.setBounds(240, 66, 42, 24);
		txtPublicPort.enableInputMethods(false);
		add(txtPublicPort);

		startButton.setBounds(36, 102, 96, 30);
		exitButton.setBounds(156, 102, 96, 30);
	}

	public static void main(String[] args) throws Exception {
		UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		UIManager.put("AuditoryCues.playList", UIManager.get("AuditoryCues.allAuditoryCues"));
		EventQueue.invokeLater(new Runnable() {
			@Override
			public void run() {
				new PortMapClientFrame().setVisible(true);
			}
		});
	}
}