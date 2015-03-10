package com.datatorrent.KafkaProducer;

import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * @author Chaitanya
 */
class KafkaDataProducer implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(KafkaDataProducer.class);
  private final kafka.javaapi.producer.Producer<String, String> producer;
  private final String topic;
  private int sendCount = 20;
  private final Random rand = new Random();
  private boolean hasPartition = false;
  private List<String> messages;
  private int startId = 1;

  private String producerType = "async";

  public int getSendCount()
  {
    return sendCount;
  }

  public void setSendCount(int sendCount)
  {
    this.sendCount = sendCount;
  }

  public void setMessages(List<String> messages)
  {
    this.messages = messages;
  }

  private ProducerConfig createProducerConfig(String brokerlist)
  {
    Properties props = new Properties();
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    props.setProperty("key.serializer.class", "kafka.serializer.StringEncoder");
    if (hasPartition) {
      props.put("metadata.broker.list", brokerlist);
      props.setProperty("partitioner.class", KafkaPartitioner.class.getCanonicalName());
    } else {
      props.put("metadata.broker.list", brokerlist);
    }
    props.setProperty("topic.metadata.refresh.interval.ms", "20000");

    props.setProperty("producer.type", getProducerType());

    return new ProducerConfig(props);
  }

  public KafkaDataProducer(String topic, String brokerlist)
  {
    this(topic, brokerlist, false);
  }

  public KafkaDataProducer(String topic, String brokerlist, boolean hasPartition)
  {
    // Use random partitioner. Don't need the key type. Just set it to Integer.
    // The message is of type String.
    this.topic = topic;
    this.hasPartition = hasPartition;
    producer = new Producer<String, String>(createProducerConfig(brokerlist));
  }

  private void generateMessages()
  {
    int messageNo = startId;
    int count = 0;
    while (count < sendCount) {
      String messageStr = "Message_" + messageNo;
      int k = rand.nextInt(100);
      producer.send(new KeyedMessage<String, String>(topic, "" + k, messageStr));
      messageNo++;
      count++;
    }
  }

  public void run()
  {
    if (messages == null) {
      generateMessages();
    } else {
      for (String msg : messages) {
        producer.send(new KeyedMessage<String, String>(topic, "", msg));
      }
    }
  }

  public void close()
  {
    producer.close();
  }

  public String getProducerType()
  {
    return producerType;
  }

  public void setProducerType(String producerType)
  {
    this.producerType = producerType;
  }

  public void setStartId(int _startId)
  {
    this.startId = _startId;
  }
}
