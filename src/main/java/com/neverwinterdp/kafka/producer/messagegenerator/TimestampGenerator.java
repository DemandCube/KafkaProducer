package com.neverwinterdp.kafka.producer.messagegenerator;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimestampGenerator implements MessageGenerator{

  

  @Override
  public String next() {
    SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss:SSSS");
    return dateFormat.format(new Date());
  }

}
