package edu.vtc.cis4250.kafka

import java.util.Properties
import java.util.Random
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class RandomProducer {
  val props: Properties = new Properties()

  // Set various properties.
  props.setProperty("bootstrap.servers", "localhost:9092")
  props.setProperty("acks", "all")
  props.setProperty("retries", "0")
  props.setProperty("batch.size", "16384")
  props.setProperty("linger.ms", "1")

  // This specifies the serializers for keys and values.
  props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.setProperty("value.serializer", "org.apache.kafka.common.serialization.DoubleSerializer")

  val producer = new KafkaProducer[String, Double](props)

  def publishMessages(topic: String, messageCount: Int): Unit = {
    val generator = new Random(0)

    for (mCount <- 0 until messageCount) {
      val msg: Double = generator.nextDouble()
      // println(msg)

      // Creates a ProducerRecord instance and publish it.
      // val data = new ProducerRecord[String, Double](topic, msg)
    
      if (msg > 0.5) { // if the msg is > 0.5 put it into the first partition
        val data = new ProducerRecord[String, Double](topic, 0, topic, msg)
        producer.send(data)
      } else {  // if the msg is <= 0.1 put it into the second partition
        val data = new ProducerRecord[String, Double](topic, 1, topic, msg)
        producer.send(data)
      } 
    }
    // Close producer connection with broker.
    producer.close()
  }
}


object RandomProducer {

  def main(args: Array[String]): Unit = {
    val argsCount = args.length
    if (argsCount != 2)
      throw new IllegalArgumentException("Provide the topic name and Message count.")

    // Topic name and the number of messages to be published are passed from the command line.
    val topic = args(0)
    val count = args(1)
    val messageCount = Integer.parseInt(count)
    println("Topic Name - " + topic)
    println("Message Count - " + messageCount)

    val randomProducer = new RandomProducer()
    randomProducer.publishMessages(topic, messageCount)
  }

}
