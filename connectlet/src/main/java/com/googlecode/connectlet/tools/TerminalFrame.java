package com.googlecode.connectlet.tools;

import java.awt.EventQueue;
import java.awt.Frame;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.util.Calendar;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ScrollPaneConstants;
import javax.swing.UIManager;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import com.googlecode.connectlet.Connection;

public class TerminalFrame extends ConnectorFrame {
	private static final long serialVersionUID = 1L;

	private static final int STATUS_CONNECTING = 0;
	private static final int STATUS_CONNECTED = 1;
	private static final int STATUS_DISCONNECTED = 2;

	static String now() {
		Calendar cal = Calendar.getInstance();
		int hour = cal.get(Calendar.HOUR_OF_DAY);
		int minute = cal.get(Calendar.MINUTE);
		int second = cal.get(Calendar.SECOND);
		return String.format("%02d:%02d:%02d", Integer.valueOf(hour),
				Integer.valueOf(minute), Integer.valueOf(second));
	}

	private JTextField txtHost = new JTextField("localhost");
	private JTextField txtPort = new JTextField("23");

	JButton btnConnect = new JButton("Connect");
	JTextField txtName = new JTextField("My Name");
	JCheckBox chkAnonymous = new JCheckBox("Anonymous");
	JCheckBox chkQuiet = new JCheckBox("Quiet");
	JButton btnSend = new JButton("Send");
	JTextArea txtRecv = new JTextArea();
	JTextArea txtSend = new JTextArea();
	int status;
	Connection connection;

	void send() {
		String msgToSend = txtSend.getText();
		if (!chkAnonymous.isSelected()) {
			connection.send(String.format("[%s Sent at %s]\r\n%s\r\n",
					txtName.getText(), now(), msgToSend).getBytes());
		} else if (!msgToSend.isEmpty()) {
			connection.send(msgToSend.getBytes());
		}
		txtSend.setText("");
	}

	void stop() {
		txtHost.setEnabled(true);
		txtPort.setEnabled(true);
		btnConnect.setText("Connect");
		btnConnect.setEnabled(true);
		btnSend.setEnabled(false);
	}

	@Override
	protected void start() {
		txtHost.setEnabled(false);
		txtPort.setEnabled(false);
		btnConnect.setText("Disconnect");
		btnConnect.setEnabled(false);
		status = STATUS_CONNECTING;
		connection = new Connection() {
			@Override
			protected void onRecv(byte[] b, int off, int len) {
				txtRecv.append(new String(b, off, len));
				txtRecv.setCaretPosition(txtRecv.getDocument().getLength());
				if (!chkQuiet.isSelected()) {
					toFront();
					Runnable sound = (Runnable) Toolkit.getDefaultToolkit().
							getDesktopProperty(UIManager.
							getString("OptionPane.informationSound"));
					new Thread(sound).start();
				}
			}

			@Override
			protected void onConnect() {
				status = STATUS_CONNECTED;
				btnConnect.setEnabled(true);
				btnSend.setEnabled(true);
				if (!chkAnonymous.isSelected()) {
					send(String.format("[%s Connected at %s]\r\n",
							txtName.getText(), now()).getBytes());
				}
			}

			@Override
			protected void onDisconnect() {
				stop();
				if (status == STATUS_DISCONNECTED) {
					return;
				}
				EventQueue.invokeLater(new Runnable() {
					@Override
					public void run() {
						if (status == STATUS_CONNECTED) {
							JOptionPane.showMessageDialog(TerminalFrame.this,
									"Connection Lost", getTitle(),
									JOptionPane.INFORMATION_MESSAGE);
						} else {
							JOptionPane.showMessageDialog(TerminalFrame.this,
									"Connection Failed", getTitle(),
									JOptionPane.WARNING_MESSAGE);
						}
					}
				});
			}
		};
		try {
			connector.connect(connection, txtHost.getText(),
					Integer.parseInt(txtPort.getText()));
		} catch (Exception e) {
			stop();
			JOptionPane.showMessageDialog(TerminalFrame.this, e.getMessage(),
					getTitle(), JOptionPane.WARNING_MESSAGE);
		}
	}

	@Override
	protected void windowClosed() {
		// Clear all non-focusable windows including MethodInputJFrame
		for (Frame frame : getFrames()) {
			if (!frame.isFocusableWindow()) {
				frame.dispose();
			}
		}
	}

	public TerminalFrame() {
		super("Terminal", "Terminal", 384, 288, false);

		JLabel lblHost = new JLabel("Host");
		lblHost.setBounds(12, 6, 36, 24);
		add(lblHost);

		txtHost.setBounds(48, 6, 114, 24);
		txtHost.enableInputMethods(false);
		add(txtHost);

		JLabel lblPort = new JLabel("Port");
		lblPort.setBounds(174, 6, 36, 24);
		add(lblPort);

		txtPort.setBounds(210, 6, 48, 24);
		txtPort.enableInputMethods(false);
		add(txtPort);

		btnConnect.setBounds(270, 6, 96, 30);
		btnConnect.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				if (btnSend.isEnabled()) {
					if (!chkAnonymous.isSelected()) {
						connection.send(String.format("[%s Disconnected at %s]\r\n",
								txtName.getText(), now()).getBytes());
					}
					status = STATUS_DISCONNECTED;
					connection.disconnect();
				} else {
					start();
				}
			}
		});
		add(btnConnect);

		JLabel lblName = new JLabel("Name");
		lblName.setBounds(12, 36, 36, 24);
		add(lblName);

		txtName.setBounds(48, 36, 114, 24);
		add(txtName);

		chkAnonymous.setBounds(168, 36, 96, 24);
		chkAnonymous.addChangeListener(new ChangeListener() {
			@Override
			public void stateChanged(ChangeEvent e) {
				txtName.setEnabled(!chkAnonymous.isSelected());
			}
		});
		add(chkAnonymous);

		chkQuiet.setBounds(264, 36, 96, 24);
		add(chkQuiet);

		txtRecv.setEditable(false);
		txtRecv.setFont(getFont());
		txtRecv.setLineWrap(true);
		txtRecv.addKeyListener(new KeyAdapter() {
			@Override
			public void keyPressed(KeyEvent e) {
				if (e.getKeyCode() == KeyEvent.VK_ESCAPE) {
					dispose();
				}
			}
		});
		JScrollPane spRecv = new JScrollPane(txtRecv,
				ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS,
				ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
		spRecv.setBounds(6, 66, 372, 114);
		add(spRecv);

		txtSend.setFont(getFont());
		txtSend.setLineWrap(true);
		txtSend.addKeyListener(new KeyAdapter() {
			@Override
			public void keyPressed(KeyEvent e) {
				if (e.getKeyCode() == KeyEvent.VK_ESCAPE) {
					dispose();
					return;
				}
				if (btnSend.isEnabled() && e.getKeyCode() == KeyEvent.VK_ENTER &&
						e.isControlDown()) {
					send();
				}
			}
		});
		JScrollPane spSend = new JScrollPane(txtSend,
				ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS,
				ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
		spSend.setBounds(6, 186, 372, 60);
		add(spSend);

		btnSend.setBounds(36, 252, 84, 30);
		btnSend.setEnabled(false);
		btnSend.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				send();
			}
		});
		add(btnSend);

		JButton btnClear = new JButton("Clear");
		btnClear.setBounds(144, 252, 84, 30);
		btnClear.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				txtRecv.setText("");
			}
		});
		add(btnClear);

		startButton.setVisible(false);
		exitButton.setBounds(252, 252, 84, 30);
	}

	public static void main(String[] args) throws Exception {
		UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		UIManager.put("AuditoryCues.playList",
				UIManager.get("AuditoryCues.allAuditoryCues"));
		EventQueue.invokeLater(new Runnable() {
			@Override
			public void run() {
				new TerminalFrame().setVisible(true);
			}
		});
	}
}