
import java.io.IOException
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class LogMapper extends Mapper[LongWritable, Text, Text, FloatWritable] {

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, FloatWritable]#Context): Unit = {
    val line = value.toString
    val fields = line.split(",")

    // from the fields, get the time and bytes transfered
    val time = fields(4)

    val bytes = fields(7).toFloat

    context.write(new Text(time), new FloatWritable(bytes))
  }
}
