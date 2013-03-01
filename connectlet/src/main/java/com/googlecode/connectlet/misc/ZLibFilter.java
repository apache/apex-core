package com.googlecode.connectlet.misc;

import java.io.ByteArrayInputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import com.googlecode.connectlet.ByteArrayQueue;
import com.googlecode.connectlet.Bytes;
import com.googlecode.connectlet.Connector;
import com.googlecode.connectlet.Filter;
import com.googlecode.connectlet.packet.PacketException;
import com.googlecode.connectlet.packet.PacketFilter;
import com.googlecode.connectlet.packet.PacketParser;

/**
 * A {@link Filter} which compresses application data and uncompresses network data.
 */
public class ZLibFilter extends PacketFilter
{
  private static PacketParser parser = new PacketParser()
  {
    @Override
    public int getPacketSize(byte[] b, int off, int len) throws PacketException
    {
      if (len < 4) {
        return 0;
      }
      if (Bytes.toShort(b, off + 2) % 31 != 0) {
        throw new PacketException("Wrong Packet Head");
      }
      int size = Bytes.toShort(b, off) & 0xffff;
      if (size < 4) {
        throw new PacketException("Wrong Packet Size");
      }
      return size;
    }

  };

  /**
   * Creates a ZLibFilter.
   */
  public ZLibFilter()
  {
    super(parser);
  }

  @Override
  public void onData(byte[] b, int off, int len)
  {
    ByteArrayQueue baq = new ByteArrayQueue();
    byte[] buffer = new byte[2048];
    try {
      InflaterInputStream inflater = new InflaterInputStream(new ByteArrayInputStream(b, off + 2, len - 2));
			int bytesRead;
      while ((bytesRead = inflater.read(buffer)) > 0) {
        baq.add(buffer, 0, bytesRead);
        // Prevent attack
        if (baq.length() > Connector.MAX_BUFFER_SIZE) {
          break;
        }
      }
      inflater.close();
    }
    catch (Exception e) {
      getConnection().disconnect();
      return;
    }
    super.onData(baq.array(), baq.offset(), baq.length());
  }

  private static final byte[] EMPTY_HEAD = new byte[] {0, 0};

  @Override
  protected void send(byte[] b, int off, int len)
  {
    ByteArrayQueue baq = new ByteArrayQueue();
    baq.add(EMPTY_HEAD);
    try  {
      DeflaterOutputStream dos = new DeflaterOutputStream(baq.getOutputStream());
      dos.write(b, off, len);
      dos.close();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    Bytes.setShort(baq.length(), baq.array(), baq.offset());
    super.send(baq.array(), baq.offset(), baq.length());
  }

}