package com.googlecode.connectlet.tools;

import java.awt.Component;
import java.awt.EventQueue;
import java.awt.Font;
import java.awt.Image;
import java.awt.Insets;
import java.awt.MenuItem;
import java.awt.PopupMenu;
import java.awt.SystemTray;
import java.awt.TrayIcon;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.InputStream;
import java.util.Arrays;

import javax.imageio.ImageIO;
import javax.swing.JButton;
import javax.swing.JFrame;

import com.googlecode.connectlet.Connector;

public abstract class ConnectorFrame extends JFrame
{
  private static final long serialVersionUID = 1L;

  protected abstract void start();

  protected void windowClosed()
  {/**/

  }

  protected TrayIcon trayIcon;
  protected MenuItem startMenuItem = new MenuItem("Start");
  protected JButton startButton = new JButton("Start");
  protected JButton exitButton = new JButton("Exit");
  protected Connector connector = new Connector();
  private Insets insets = new Insets(0, 0, 0, 0);
  private KeyAdapter keyAdapter = new KeyAdapter()
  {
    @Override
    public void keyPressed(KeyEvent e)
    {
      if (e.getKeyCode() == KeyEvent.VK_ESCAPE) {
        dispose();
      }
    }

  };

  @Override
  public Component add(Component comp)
  {
    if (comp instanceof JButton) {
      ((JButton)comp).setMargin(insets);
    }
    if (comp.isFocusable()) {
      comp.addKeyListener(keyAdapter);
    }
    return super.add(comp);
  }

  protected ConnectorFrame(String title, String icon,
          final int width, final int height, final boolean tray)
  {
    super(title);
    setLayout(null);
    setLocationByPlatform(true);
    setResizable(false);

    startButton.addActionListener(new ActionListener()
    {
      @Override
      public void actionPerformed(ActionEvent e)
      {
        start();
      }

    });
    add(startButton);
    exitButton.addActionListener(new ActionListener()
    {
      @Override
      public void actionPerformed(ActionEvent e)
      {
        dispose();
      }

    });
    add(exitButton);
    addWindowListener(new WindowAdapter()
    {
      @Override
      public void windowOpened(WindowEvent e)
      {
        Insets i = getInsets();
        setSize(width + i.left + i.right, height + i.top + i.bottom);
      }

      @Override
      public void windowIconified(WindowEvent e)
      {
        if (tray && SystemTray.isSupported()) {
          setVisible(false);
          try {
            SystemTray.getSystemTray().add(trayIcon);
          }
          catch (Exception ex) {
            throw new RuntimeException(ex);
          }
        }
      }

      @Override
      public void windowClosing(WindowEvent e)
      {
        dispose();
      }

      @Override
      public void windowClosed(WindowEvent e)
      {
        running = false;
        ConnectorFrame.this.windowClosed();
      }

    });

    Class<ConnectorFrame> clazz = ConnectorFrame.class;
    try {
      InputStream in16 = clazz.getResourceAsStream(icon + "Icon16.gif");
      InputStream in32 = clazz.getResourceAsStream(icon + "Icon32.gif");
      InputStream in48 = clazz.getResourceAsStream(icon + "Icon48.gif");
      setIconImages(Arrays.asList(new Image[] {ImageIO.read(in16),
                                               ImageIO.read(in32), ImageIO.read(in48)}));
      in16.close();
      in32.close();
      in48.close();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (!tray) {
      return;
    }
    MenuItem miOpen = new MenuItem("Open"), miExit = new MenuItem("Exit");
    Font font = startButton.getFont();
    miOpen.setFont(new Font(font.getName(), Font.BOLD, font.getSize()));
    startMenuItem.setFont(font);
    miExit.setFont(font);
    PopupMenu popup = new PopupMenu();
    popup.add(miOpen);
    popup.add(startMenuItem);
    popup.add(miExit);
    try  {
      InputStream in = clazz.getResourceAsStream(icon + "Icon16.gif");
      trayIcon = new TrayIcon(ImageIO.read(in), getTitle(), popup);
      in.close();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    trayIcon.addMouseListener(new MouseAdapter()
    {
      @Override
      public void mouseClicked(MouseEvent e)
      {
        if (e.getButton() == MouseEvent.BUTTON1) {
          SystemTray.getSystemTray().remove(trayIcon);
          setVisible(true);
          setState(NORMAL);
        }
      }

    });

    miOpen.addActionListener(new ActionListener()
    {
      @Override
      public void actionPerformed(ActionEvent e)
      {
        SystemTray.getSystemTray().remove(trayIcon);
        setVisible(true);
        setState(NORMAL);
      }

    });
    startMenuItem.addActionListener(new ActionListener()
    {
      @Override
      public void actionPerformed(ActionEvent e)
      {
        start();
      }

    });
    miExit.addActionListener(new ActionListener()
    {
      @Override
      public void actionPerformed(ActionEvent e)
      {
        SystemTray.getSystemTray().remove(trayIcon);
        dispose();
      }

    });
  }

  protected void doEvents()
  {
    if (connector.getBufferSize() == Connector.MAX_BUFFER_SIZE) {
      while (connector.doEvents()) {/**/

      }
    }
    else {
      connector.doEvents();
    }
  }

  boolean running = false;

  @Override
  public void setVisible(boolean b)
  {
    super.setVisible(b);
    if (!b || running) {
      return;
    }
    running = true;
    EventQueue.invokeLater(new Runnable()
    {
      @Override
      public void run()
      {
        doEvents();
        try {
          Thread.sleep(16);
        }
        catch (InterruptedException e) {/**/

        }
        if (running) {
          EventQueue.invokeLater(this);
        }
        else {
          connector.close();
        }
      }

    });
  }

}