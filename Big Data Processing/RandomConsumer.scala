package edu.vtc.cis4250.kafka

import java.time.Duration
import java.util.{Collections, Properties}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import scala.collection.JavaConverters._
import org.apache.kafka.common.TopicPartition

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

  val consumer = new KafkaConsumer[String, Double](props)
  consumer.subscribe(Collections.singletonList(topic))

  def consumeMessages(part: Int): Unit = {
    while (true) {
      // records come from the partition number inputed to the function via part
      var partition : TopicPartition = new TopicPartition(topic, part) 
      consumer.position(partition)
      val records: ConsumerRecords[String, Double] = consumer.poll(Duration.ofMillis(1000))

      var total : Double = 0 // total all the numbers together
      var count = 0 // count all values for average calculation

      for (record <- records.asScala) {
        printf("partition %f: offset = %10d, value %f%n", part, record.offset(), record.value())
        total = total + record.value()
        count = count + 1
      }

      var avg = total / count
      printf("partition %f average: &f%n", part, avg)
    }

    // The statement below is unreachable.
    // consumer.close();
  }
}


object RandomConsumer {

  def main(args: Array[String]): Unit = {
    val argsCount = args.length
    if (argsCount != 1)
      throw new IllegalArgumentException("Provide the topic name.")

    val topic = args(0)
    println("Topic Name - " + topic)

    val randomHLConsumer = new RandomConsumer(topic)
    val randomHLConsumer2 = new RandomConsumer(topic)
    randomHLConsumer.consumeMessages(1)
    randomHLConsumer2.consumeMessages(2)
  }

}
