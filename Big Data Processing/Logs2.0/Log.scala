
import scala.reflect.ClassTag
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{FloatWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.{Tool, ToolRunner}

class Log extends Configured with Tool {

  override def run(args: Array[String]): Int = {
    if (args.length != 2) {
      System.err.println("Usage: Log <input path> <output path>")
      1
    }
    else {
      val conf = getConf()
      val job = Job.getInstance(conf, "Log")
      job.setJar("Log.jar")
        
      FileInputFormat.addInputPath(job, new Path(args(0)))
      FileOutputFormat.setOutputPath(job, new Path(args(1)))

      job.setMapperClass(classOf[LogMapper])
      job.setReducerClass(classOf[LogReducer])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[FloatWritable])

      if (job.waitForCompletion(true)) 0 else 1;
    }
  }
}


object Log {

  def main(args: Array[String]): Unit = {
    val result = ToolRunner.run(new Configuration, new Log, args)
    System.exit(result)
  }

}
