/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.tigerby.storm.perftest.generated;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public interface DataProtocol {
  public static final org.apache.avro.Protocol PROTOCOL = org.apache.avro.Protocol.parse("{\"protocol\":\"DataProtocol\",\"namespace\":\"com.tigerby.storm.perftest.generated\",\"types\":[{\"type\":\"enum\",\"name\":\"Status\",\"symbols\":[\"SUCCESS\",\"FAIL\",\"UNKNOWN\"]}],\"messages\":{\"execute\":{\"request\":[{\"name\":\"data\",\"type\":\"string\"}],\"response\":\"Status\"}}}");
  com.tigerby.storm.perftest.generated.Status execute(java.lang.CharSequence data) throws org.apache.avro.AvroRemoteException;

  @SuppressWarnings("all")
  public interface Callback extends DataProtocol {
    public static final org.apache.avro.Protocol PROTOCOL = com.tigerby.storm.perftest.generated.DataProtocol.PROTOCOL;
    void execute(java.lang.CharSequence data, org.apache.avro.ipc.Callback<com.tigerby.storm.perftest.generated.Status> callback) throws java.io.IOException;
  }
}