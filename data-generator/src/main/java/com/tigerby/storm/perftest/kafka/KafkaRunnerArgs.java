package com.tigerby.storm.perftest.kafka;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class KafkaRunnerArgs {
  @Option(name = "--help", usage = "show help")
  public boolean help_ = false;

  @Option(name = "--brokers_", aliases = "-b", usage = "broker list")
  public String brokers_ = "daisy11:9091,daisy11:9092,daisy12:9093,daisy12:9094";

  @Option(name = "--rate", aliases = "-r", usage = "message generating rate.")
  public int rate_ = 1000;

//  @Option(name="--messageSizeByte", aliases={"--messageSize"}, metaVar="SIZE",
//          usage="size of the messages generated in bytes")
//  public int messageSize_ = 500;

  @Option(name="--numberOfThreads", aliases={"--threadNum"}, metaVar="SIZE",
          usage="number of the threads operate generating messages.")
  public int threadNum_ = 1;

  public static KafkaRunnerArgs getValidatedArgs(String[] args) {
    KafkaRunnerArgs arg = new KafkaRunnerArgs();

    CmdLineParser cmdLineParser = new CmdLineParser(arg);
    cmdLineParser.setUsageWidth(80);
    try {
      cmdLineParser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      arg.help_ = true;
    }

    if (arg.help_) {
      cmdLineParser.printUsage(System.err);
      System.err.println();
      System.exit(1);
    }

    return arg;
  }

}
