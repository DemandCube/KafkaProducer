package com.neverwinterdp.kafka.producer.messagegenerator;

public class IntegerGenerator implements MessageGenerator{
  private int currNum;
  public IntegerGenerator(){
    currNum = 0;
  }
  
  @Override
  public String next() {
    return Integer.toString(currNum++);
  }

}
