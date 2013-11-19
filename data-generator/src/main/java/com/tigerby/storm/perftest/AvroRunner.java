package com.tigerby.storm.perftest;


import com.tigerby.storm.perftest.avro.AvroRunnerArgs;
import com.tigerby.storm.perftest.avro.AvroSender;
import com.tigerby.storm.perftest.commons.Regulator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class AvroRunner {

  ExecutorService executorService = Executors.newCachedThreadPool(Executors.defaultThreadFactory());

  public static void main(String[] args) {
    AvroRunner runner = new AvroRunner();
    runner.start(args);
  }

  private void start(String[] args) {
    AvroRunnerArgs arg = AvroRunnerArgs.getValidatedArgs(args);

    for(int i=0; i<arg.threadNum_; i++) {
      Regulator regulator = new Regulator(String.valueOf(i+1), arg.rate_, new AvroSender(arg.host_, arg.port_, arg.messageSize_));
      executorService.submit(regulator);
    }
  }
}
