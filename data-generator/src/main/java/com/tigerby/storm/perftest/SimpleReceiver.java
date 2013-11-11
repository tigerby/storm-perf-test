package com.tigerby.storm.perftest;

import com.tigerby.storm.perftest.generated.DataProtocol;
import com.tigerby.storm.perftest.generated.Status;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class SimpleReceiver {

  private static Server server;

  public static void main(String[] args) {
    startServer();
  }

  public static void startServer() {
    int port = 10001;

    ExecutorService es = Executors.newCachedThreadPool();
    OrderedMemoryAwareThreadPoolExecutor executor =
        new OrderedMemoryAwareThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), 0, 0);
    ExecutionHandler executionHandler = new ExecutionHandler(executor);

    server = new NettyServer(
        new SpecificResponder(DataProtocol.Callback.class, new DataProtocolImpl()),
        new InetSocketAddress(port),
        new NioServerSocketChannelFactory(es, es),
        executionHandler
    );

    System.out.printf("Netty server starts to listen at %d\n", port);
  }

  static class DataProtocolImpl implements DataProtocol {
    long count;

    @Override
    public Status execute(CharSequence data) throws AvroRemoteException {
//        System.out.println(data.toString());
      count++;
      System.out.printf("count: %d\n", count);
      return Status.SUCCESS;
    }
  }
}
