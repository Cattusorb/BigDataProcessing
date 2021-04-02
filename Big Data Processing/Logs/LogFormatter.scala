package edu.vtc.cis4250.kafka

import java.util.Properties
import java.util.Random
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import play.api.libs.json._

// Roslyn Parker
// CIS4250
// 2 Dec 2020 - 

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

  val producer = new KafkaProducer[String, String](props)

  def publishMessages(topic: String): Unit = {
  
    // get IP of message for if statement purposes
    val IPjson: String
    val subIP: String
    
    for (line in log file)
      // get data from apache server
      // format info into JSON
      val json = """{
        "ip" : ,
        "user" : ,
        "datetime" : { "date" : , "time" : , "tz" : }, 
        "request" : { "method" : , "resource" : , "protocol" : },
        "status" : , 
        "size" : 
        }"""
      val jValue = parse(json)

      IPjson = jValue.extract[ip] 
      subIP = IPjson.substring(1, 7) 
      //JSON to string format into msg
      val msg: String = jValue  
      // println(msg)
      // Creates a ProducerRecord instance and publish it.
      if (subIP == "155.55.") { // if the msg contains IP "155.55.*.* then put it into the first partition
        val data = new ProducerRecord[String, String](topic, 0, IP, msg)
        producer.send(data)
      } else {  // all other msg with other IPs are put into the second partition
        val data = new ProducerRecord[String, String](topic, 1, IP, msg)
        producer.send(data)
      } 
    }
    // Close producer connection with broker.
    producer.close()
  }
}


object LogFormatter {

  def main(args: Array[String]): Unit = {
    val argsCount = args.length
    if (argsCount != 2)
      throw new IllegalArgumentException("Provide the topic name and Message count.")

    // Topic name and the number of messages to be published are passed from the command line.
    val topic = args(0)
    
    println("Topic Name - " + topic)
    
    val logFormatter = new LogFormatter()
    logFormatter.publishMessages(topic)
  }

}
