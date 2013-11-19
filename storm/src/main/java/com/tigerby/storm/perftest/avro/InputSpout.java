package com.tigerby.storm.perftest.avro;


import com.tigerby.storm.perftest.generated.DataProtocol;
import com.tigerby.storm.perftest.generated.Status;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class InputSpout implements IRichSpout, Runnable {

  private SpoutOutputCollector collector;
  private transient Server server;
  private int port;

  LinkedBlockingQueue<String> rows = new LinkedBlockingQueue<String>();

  private long messageCount;

  public InputSpout(int port) {
    this.port = port;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("id", "raw"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.collector = collector;

    new Thread(this).start();
  }

  @Override
  public void nextTuple() {
    if (rows.size() > 0) {
      Collection<String> list = new ArrayList<String>();
      rows.drainTo(list);
      for (String row : list) {
        collector.emit(new Values(row.substring(0, 1), row), messageCount);
        messageCount++;
      }
    }
  }

  @Override
  public void ack(Object o) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void fail(Object o) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void close() {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void activate() {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void deactivate() {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void run() {

    ExecutorService es = Executors.newCachedThreadPool();
    OrderedMemoryAwareThreadPoolExecutor
        executor =
        new OrderedMemoryAwareThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), 0, 0);
    ExecutionHandler executionHandler = new ExecutionHandler(executor);

    server = new NettyServer(
        new SpecificResponder(DataProtocol.class, new DataProtocolImpl()),
        new InetSocketAddress(port),
        new NioServerSocketChannelFactory(es, es),
        executionHandler
    );
  }

  public class DataProtocolImpl implements DataProtocol {

    @Override
    public Status execute(CharSequence data) throws AvroRemoteException {
      rows.add(data.toString());

      return Status.SUCCESS;
    }
  }

}
