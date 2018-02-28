package gdev;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
 
public class MultipleMR extends Configured implements Tool
{
    public static void main(String[] args) throws Exception
    {
       int exitCode = ToolRunner.run(new Configuration(),new MultipleMR(),args);
       System.exit(exitCode);
    }
 
 @Override
 public int run(String[] args) throws Exception {

/*
       Job job = Job.getInstance();
 *     job.setJarByClass(MyJob.class);
 *     
 *     // Specify various job-specific parameters     
 *     job.setJobName("myjob");
 *     
 *     job.setInputPath(new Path("in"));
 *     job.setOutputPath(new Path("out"));
 *     
 *     job.setMapperClass(MyJob.MyMapper.class);
 *     job.setReducerClass(MyJob.MyReducer.class);
 *
 *     // Submit the job, then poll for progress until the job is complete
 *     job.waitForCompletion(true);
*/
	 
	 //set env vars  System.setProperty("HADOOP_USER_NAME", "hadoop"); 
  
 // Job job = new Job(getConf());
  Configuration conf= new Configuration();
  // session.id is deprecated. Instead, use dfs.metrics.session-id
  conf.setStrings("dfs.metrics.session-id", "1234567890");	  
  
  Job job = Job.getInstance(conf);
  job.setJobName("MultipleOutputFormat example");
  job.setJarByClass(MultipleMR.class);

  LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

  Path hdfs_uri_in  = new Path("hdfs://10.242.5.88:9000/user/data/test_data.csv");
  Path hdfs_uri_out = new Path("hdfs://10.242.5.88:9000/user/data/mmr");
  
  FileInputFormat.setInputPaths(job, hdfs_uri_in);
  FileOutputFormat.setOutputPath(job, hdfs_uri_out);
   
  job.setMapperClass(MultipleOutputMapper.class);
  job.setMapOutputKeyClass(Text.class);
  
  job.setReducerClass(MultiOutputReducer.class);
  
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(Text.class);
  job.setNumReduceTasks(5);
   
  boolean success = job.waitForCompletion(true);
  return success ? 0 : 1;
 }
}