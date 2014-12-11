package com.neverwinterdp.kafka.producer;


public class BytesEncoder implements kafka.serializer.Encoder<byte[]> {
  @Override
  public byte[] toBytes(byte[] bytes) {
    return bytes;
  }
}
