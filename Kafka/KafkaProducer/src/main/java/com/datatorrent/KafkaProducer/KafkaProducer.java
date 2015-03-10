package com.datatorrent.KafkaProducer;

/**
 * @author Chaitanya
 */
public class KafkaProducer
{
  public static void main(String[] args)
  {
    KafkaDataProducer p = new KafkaDataProducer(args[0], args[1], Boolean.valueOf(args[2]));
    p.setSendCount(Integer.valueOf(args[3]));
    p.setStartId(Integer.valueOf(args[4]));
    new Thread(p).start();

  }
}