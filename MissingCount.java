/*
This is the Driver code for the MissingCount Job 
*/
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

@SuppressWarnings("unused")
public class MissingCount
{
 public static void main(String[] args) throws Exception
 {
	//Configuration conf details w.r.t to JOB & JAR
   Configuration conf = new Configuration();
   @SuppressWarnings("deprecation")
   Job job = new Job(conf);
   
   job.setJobName("Missing Count");
   
   //this will take only the driver class
   job.setJarByClass(MissingCount.class);
	
	//mapper & reducer class Name details
   job.setMapperClass(MissingCountMapper.class);
   job.setReducerClass(MissingCountReducer.class);
	
	//Final output key and value datatypes details
   job.setMapOutputKeyClass(Text.class);
   job.setMapOutputValueClass(IntWritable.class);

   job.setOutputKeyClass(Text.class);
   job.setOutputValueClass(Text.class);
   
   job.setInputFormatClass(TextInputFormat.class);
   job.setOutputFormatClass(TextOutputFormat.class);

   //HDFS input and output path details
   FileInputFormat.addInputPath(job, new Path(args[0]));
   FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	//how the execution triggers
   System.exit(job.waitForCompletion(true) ? 0:1);

 }
}