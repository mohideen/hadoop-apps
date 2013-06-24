package edu.umd.lib.hadoopapps;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MD5Batch extends Configured implements Tool {
  
   public static class MD5BatchMapper extends Mapper<Text, NullWritable, Text, Text>{
    
    Path inputPath;
    InputStream inStream;
    FileSystem hdfs;
    Text hashText = new Text();
    
    @Override
    protected void setup(Context context) throws IOException ,InterruptedException {
      hdfs = FileSystem.get(context.getConfiguration());
    }
    
    @Override
    protected void map(Text key, NullWritable value, Context context) 
        throws IOException ,InterruptedException {
      
      inputPath = new Path(key.toString());
      inStream = (hdfs.open(inputPath));
      MD5Hash md5hash = MD5Hash.digest(inStream);
      hashText.set(md5hash.toString());
      context.write(hashText, key);
    }
   
  }
  
  private void usage() {
    System.err.println("Usage: MD5Batch inputFile outputFile numMaps ");
    ToolRunner.printGenericCommandUsage(System.out);
  }

  public int run(String[] args) throws Exception {
    
    if(args.length < 3) {
      usage();
      System.exit(-1);
    }
    
    String inputFile = args[0];
    String outputDir = args[1];
    String numMaps = args[2];

    Path inputPath = new Path(inputFile);
    Path outputPath = new Path(outputDir);
    
    Job job = Job.getInstance(getConf());
    job.setJobName("MD5Batch");
    
    //job.setInputFormatClass(S.class);
    job.setJarByClass(MD5Batch.class);
    
    job.setMapperClass(MD5BatchMapper.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.getConfiguration().set(JobContext.MAP_SPECULATIVE, "false");
    job.getConfiguration().set(JobContext.NUM_MAPS, numMaps);
    
    SequenceFileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    
    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Completed in " +
        ((System.currentTimeMillis() - startTime)/1000) + "secs.");
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MD5Batch(), args);
    System.exit(res);
  }
  
}
