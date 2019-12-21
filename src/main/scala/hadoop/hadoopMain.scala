package hadoop
import java.lang
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobConf}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
object hadoopMain {
//  val conf = new Configuration()
//  def main(args: Array[String]): Unit = {
    val jobConf = new JobConf(new Configuration(), classOf[Job])
    val job = Job.getInstance(jobConf, "wordCount")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[mapper])
    job.setCombinerClass(classOf[reducer])
    job.setReducerClass(classOf[reducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(jobConf, new Path("../../../anagram.txt"))
    FileOutputFormat.setOutputPath(jobConf, new Path("hadoopmain.txt"))
    System.exit(if(job.waitForCompletion(true)) 0 else 1)
//      }
}

class reducer extends Reducer[Text, IntWritable, Text, IntWritable]{
    val result = new IntWritable()
  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    super.reduce(key, values, context)
    var sum = 0
    values.forEach(x => sum+= x.get())
    result.set(sum)
    context.write(key, result)
  }
}


class mapper extends Mapper[IntWritable, Text, Text, IntWritable]{
  val initialVal = new IntWritable(1)
  val word = new Text
  override def map(key: IntWritable, value: Text, context: Mapper[IntWritable, Text, Text, IntWritable]#Context): Unit = {
    super.map(key, value, context)
    val iterator = value.toString.split(" ")
    var j = 0
    while (!iterator(j).isEmpty){
      word.set(iterator(j))
      context.write(word, initialVal)
      j+=1
    }
  }
}
