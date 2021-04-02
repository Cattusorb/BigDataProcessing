
import java.io.IOException
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConverters._

class LogReducer extends Reducer[Text, FloatWritable, Text, FloatWritable] {

  override def reduce(key: Text, values: java.lang.Iterable[FloatWritable], context: Reducer[Text, FloatWritable]#Context): Unit = {
    val bytes = new Array[Double](24)
    for (value <- values.asScala) {
      for (i <- 0 until 24){ // from hour 0 until 24
        if (key.substring(0, 1) == "0") { // if the time is < 10
          if (key.substring(1, 2) == i.toString) { // get the second value of key
            bytes(i) = bytes(i) + value.get()
          }
        } else { // else if the time is >= 10
            if (key == i.toString) { // get the whole key 
            bytes(i) = bytes(i) + value.get()
          }
        }
      }
    }
    
    for (i <- 0 until 24) {
      context.write(key, new FloatWritable(bytes(i))
    }
  }
}
