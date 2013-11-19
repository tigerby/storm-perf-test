package com.tigerby.storm.perftest.avro;

import com.tigerby.storm.perftest.commons.Sender;
import com.tigerby.storm.perftest.generated.DataProtocol;

import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class AvroSender implements Sender {

  private static final
  Character[] CHARS =
      new Character[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
                      'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};
  private static Random _random = new Random();

  NettyTransceiver _transceiver;
  public DataProtocol _proxy;

  private String _host;
  private int _port;
  private long _messageSize;



  public AvroSender(String host, int port) {
    _host = host;
    _port = port;
    connect();
  }

  public AvroSender(String host, int port, long messageSize) {
    _host = host;
    _port = port;
    _messageSize = messageSize;

    connect();
  }

  public void connect() {
    try {
      while(true) {
        try {
          _transceiver = new NettyTransceiver(new InetSocketAddress(_host, _port));
          break;
        } catch (IOException e) {
          System.out.printf("%s, cause: %s\n", e.getMessage(), e.getCause());
          Thread.sleep(5000L);
        }
      }
      _proxy = SpecificRequestor.getClient(DataProtocol.class, _transceiver);
    } catch (Exception e) {
      System.out.printf("%s, cause: %s", e.getMessage(), e.getCause());
    }
  }

  @Override
  public void send() {
    try {
      _proxy.execute(randString(_messageSize));
    } catch (Exception e) {
      if(!_transceiver.isConnected()) {
        _transceiver.close();
        connect();
      }
    }

  }

  private String randString(long size) {
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < size; i++) {
      buf.append(CHARS[_random.nextInt(CHARS.length)]);
    }
    return buf.toString();
  }
}
