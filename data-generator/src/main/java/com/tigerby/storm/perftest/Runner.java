package com.tigerby.storm.perftest;


import com.tigerby.storm.perftest.Sender.AvroSender;
import com.tigerby.storm.perftest.regulator.Regulator;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class Runner {
  @Option(name = "--help", usage = "show help")
  private boolean help = false;

  @Option(name = "--host", aliases = "-h", usage = "hostname which send message to.")
  private String host = "localhost";

  @Option(name = "--port", aliases = "-p", usage = "port which send message to.")
  private int port = 10001;

  @Option(name = "--rate", aliases = "-r", usage = "message generating rate.")
  private int rate = 1000;

  @Option(name="--messageSizeByte", aliases={"--messageSize"}, metaVar="SIZE",
          usage="size of the messages generated in bytes")
  private int _messageSize = 500;

  @Option(name="--numberOfThreads", aliases={"--threadNum"}, metaVar="SIZE",
          usage="number of the threads operate generating messages.")
  private int _threadNum = 1;


  ExecutorService executorService = Executors.newCachedThreadPool(Executors.defaultThreadFactory());

  public static void main(String[] args) {
    Runner runner = new Runner();
    runner.start(args);
  }

  private void start(String[] args) {
    CmdLineParser cmdLineParser = new CmdLineParser(this);
    cmdLineParser.setUsageWidth(80);
    try {
      cmdLineParser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      help = true;
    }

    if(help) {
      cmdLineParser.printUsage(System.err);
      System.err.println();
      System.exit(1);
    }

    for(int i=0; i<_threadNum; i++) {
      Regulator regulator = new Regulator(String.valueOf(i+1), rate, new AvroSender(host, port, _messageSize));
      executorService.submit(regulator);
    }
  }
}
