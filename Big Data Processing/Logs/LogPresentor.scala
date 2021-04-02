package edu.vtc.cis4250.kafka

import java.time.Duration
import java.util.{Collections, Properties}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import scala.collection.JavaConverters._
import org.apache.kafka.common.TopicPartition

// Roslyn Parker
// CIS4250
// 2 Dec 2020 -

class RandomConsumer(topic: String) {
  val props = new Properties()

  // Set various properties.
  props.setProperty("bootstrap.servers", "localhost:9092")
  props.setProperty("group.id", "PrimaryWorkers")
  props.setProperty("enable.auto.commit", "true")
  props.setProperty("auto.commit.interval.ms", "1000")

  // This specifies the deserializers for keys and values.
  props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.DoubleDeserializer")

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(Collections.singletonList(topic))

  def consumeMessages(part: Int): Unit = {
    while (true) {
      // records come from the partition number inputed to the function via part
      var partition : TopicPartition = new TopicPartition(topic, part) 
      consumer.position(partition)
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(1000))

      for (record <- records.asScala) {
        // print out IP, method, resource path
        val jValue = parse(record.value())
        val ipaddr = jValue.extract[ip]
        val request = jValue.extract[request]
        val method = request.extract[method]
        val resource = request.extract[resource]
        printf(s"host : lemuria.cis.vtc.edu, ip : $ipaddr, method : $method, resource : $resource \n")
      }
    }

    // The statement below is unreachable.
    // consumer.close();
  }
}


object LogPresentor {

  def main(args: Array[String]): Unit = {
    val argsCount = args.length
    if (argsCount != 1)
      throw new IllegalArgumentException("Provide the topic name.")

    val topic = args(0)
    println("Topic Name - " + topic)

    val presentor1 = new LogPresentor(topic)
    val presentor2 = new LogPresentor(topic)
    presentor1.consumeMessages(1)
    presentor2.consumeMessages(2)
  }

}
