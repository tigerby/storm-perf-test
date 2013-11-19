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
public class AvroArgs {

  @Option(name = "--help", usage = "show help.")
  public boolean help_;

  @Option(name = "--port", aliases = "-p", usage = "listening port.")
  public int port_ = 10001;

  @Option(name = "--mode", aliases = "-m", usage = "running mode. local or cluster")
  public String mode_ = "local";

  @Option(name = "--spoutParallel", aliases = {"--spout"}, metaVar = "SPOUT",
          usage = "number of spouts to run in parallel")
  public int spoutParallel_ = 1;

  @Option(name = "--boltParallel", aliases = {"--bolt"}, metaVar = "BOLT",
          usage = "number of bolts to run in parallel")
  public int boltParallel_ = 1;

  @Option(name = "--numWorkers", aliases = {"--workers"}, metaVar = "WORKERS",
          usage = "number of workers to use per topology")
  public int numWorkers_ = 2;

  @Option(name = "--maxSpoutPending", aliases = {"--maxPending"}, metaVar = "PENDING",
          usage = "maximum number of pending messages per spout (only valid if acking is enabled)")
  public int maxSpoutPending_ = 1;

  @Option(name = "--messageSizeByte", aliases = {"--messageSize"}, metaVar = "SIZE",
          usage = "size of the messages generated in bytes")
  public int messageSize_ = 500;

  @Option(name = "--pollFreqSec", aliases = {"--pollFreq"}, metaVar = "POLL",
          usage = "How often should metrics be collected")
  public int pollFreqSec_ = 30;

  @Option(name = "--testTimeSec", aliases = {"--testTime"}, metaVar = "TIME",
          usage = "How long should the perftest run for.")
  public int testRunTimeSec_ = 5 * 60;

  public static AvroArgs getValidatedArgs(String[] args) {
    AvroArgs arg = new AvroArgs();

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
