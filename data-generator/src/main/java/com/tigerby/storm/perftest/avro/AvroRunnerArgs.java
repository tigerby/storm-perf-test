package com.tigerby.storm.perftest.avro;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class AvroRunnerArgs {
  @Option(name = "--help", usage = "show help")
  public boolean help_ = false;

  @Option(name = "--host", aliases = "-h", usage = "hostname which send message to.")
  public String host_ = "localhost";

  @Option(name = "--port", aliases = "-p", usage = "port which send message to.")
  public int port_ = 10001;

  @Option(name = "--rate", aliases = "-r", usage = "message generating rate.")
  public int rate_ = 1000;

  @Option(name="--messageSizeByte", aliases={"--messageSize"}, metaVar="SIZE",
          usage="size of the messages generated in bytes")
  public int messageSize_ = 500;

  @Option(name="--numberOfThreads", aliases={"--threadNum"}, metaVar="SIZE",
          usage="number of the threads operate generating messages.")
  public int threadNum_ = 1;

  public static AvroRunnerArgs getValidatedArgs(String[] args) {
    AvroRunnerArgs arg = new AvroRunnerArgs();

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
