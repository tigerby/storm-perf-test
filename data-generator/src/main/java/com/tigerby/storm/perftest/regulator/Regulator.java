package com.tigerby.storm.perftest.regulator;

import com.tigerby.storm.perftest.Sender.Sender;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class Regulator implements Runnable {

  private String _id;
  private final int _rate;
  private final Sender _sender;

  public Regulator(String _id, int _rate, Sender _sender) {
    this._id = _id;
    this._rate = _rate;
    this._sender = _sender;
  }

  @Override
  public void run() {
    int eventPer50ms = _rate / 20;
    int countLast5s = 0;
    int sleepLast5s = 0;
    long lastThroughputTick = System.currentTimeMillis();

    try {
      do {
        long ms = System.currentTimeMillis();
        for (int i = 0; i < eventPer50ms; i++) {

          _sender.send();

          countLast5s++;
          // info
          if (System.currentTimeMillis() - lastThroughputTick > 5 * 1E3) {
            System.out.printf("[%s] Sent %d in %d(ms) avg ns/msg %.0f(ns) avg %d(msg/s) sleep %d(ms)\n",
                              _id,
                              countLast5s,
                              System.currentTimeMillis() - lastThroughputTick,
                              (float) 1E6 * countLast5s / (System.currentTimeMillis()
                                                           - lastThroughputTick),
                              countLast5s / 5,
                              sleepLast5s
            );
            countLast5s = 0;
            sleepLast5s = 0;
            lastThroughputTick = System.currentTimeMillis();
          }
        }
        // rate adjust
        if (System.currentTimeMillis() - ms < 50) {
          // lets avoid sleeping if == 1ms, lets account 3ms for interrupts
          long sleep = Math.max(1, (50 - (System.currentTimeMillis() - ms) - 3));
          sleepLast5s += sleep;
          Thread.sleep(sleep);
        }
      } while (true);
    } catch (Throwable t) {
      t.printStackTrace();
      System.err.println("Error sending data to server. Did server disconnect?");
    }
  }
}
