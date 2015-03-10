package com.datatorrent.KafkaProducer;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class KafkaPartitioner implements Partitioner
{
  public KafkaPartitioner(VerifiableProperties props)
  {

  }

  public int partition(Object key, int num_Partitions)
  {
    return Integer.parseInt((String) key) % num_Partitions;
  }
}