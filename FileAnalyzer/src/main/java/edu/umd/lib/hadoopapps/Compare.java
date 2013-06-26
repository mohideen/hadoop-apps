package edu.umd.lib.hadoopapps;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.lib.hadoopapps.io.PairOfStringInt;

public class Compare extends Configured implements Tool {
  
  public static class CompareSourceMapper extends Mapper<LongWritable, Text, PairOfStringInt, Text> {
    
    PairOfStringInt keyPair = new PairOfStringInt();
    StringTokenizer tokens;
    Text valueToken = new Text();
    FileSystem hdfs;
    boolean firstColumnCompare = true;
    static String keyString = new String();
    static Text valueText = new Text();
    
    protected void setup(Context context) throws IOException ,InterruptedException {
      hdfs = FileSystem.get(context.getConfiguration());
      firstColumnCompare = context.getConfiguration().get("compareBy", "1").equalsIgnoreCase("1");
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) 
        throws java.io.IOException ,InterruptedException {
      tokens = new StringTokenizer(value.toString());
      if(firstColumnCompare) {
        keyString = tokens.nextToken();
        valueText.set(tokens.nextToken());
      } else {
        valueText.set(tokens.nextToken());
        keyString = tokens.nextToken();      
      }
      keyPair.set(keyString, 0);
      valueToken.set(valueText);
      context.write(keyPair, valueToken);
      
    }
    
  }
  
public static class CompareTargetMapper extends Mapper<LongWritable, Text, PairOfStringInt, Text> {
    
    PairOfStringInt keyPair = new PairOfStringInt();
    StringTokenizer tokens;
    Text valueToken = new Text();
    FileSystem hdfs;
    boolean firstColumnCompare = true;
    static String keyString = new String();
    static Text valueText = new Text();
    
    protected void setup(Context context) throws IOException ,InterruptedException {
      hdfs = FileSystem.get(context.getConfiguration());
      firstColumnCompare = context.getConfiguration().get("compareBy", "1").equalsIgnoreCase("1");
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) 
        throws java.io.IOException ,InterruptedException {
      tokens = new StringTokenizer(value.toString());
      if(firstColumnCompare) {
        keyString = tokens.nextToken();
        valueText.set(tokens.nextToken());
      } else {
        valueText.set(tokens.nextToken());
        keyString = tokens.nextToken();      
      }
      keyPair.set(keyString, 1);
      valueToken.set(valueText);
      context.write(keyPair, valueToken);
      
    }
  }
  
  public static class CompareReducer extends Reducer<PairOfStringInt, Text, Text, Text> {
    
    Iterator<Text> iter;
    Text valueText = new Text();
    Text keyText = new Text();
    String lastKey;
    boolean lastKeyBelongsToSrc = false;
    String currKey;
    boolean currKeyBelongsToSrc = false;
    MultipleOutputs<Text, Text> mos;
    List<String> sourceValues = new LinkedList<String>();
    String sourceValue;
    String targetValue;
    StringBuilder mergedValues = new StringBuilder();
    
    @Override
    protected void setup(Context context) 
        throws java.io.IOException ,InterruptedException {
      mos = new MultipleOutputs<Text, Text>(context);      
    }
    
    @Override
    protected void reduce(PairOfStringInt key, Iterable<Text> values, 
        Context context) throws java.io.IOException ,InterruptedException {
      boolean matched;
      currKey = key.getLeftElement();
      currKeyBelongsToSrc = (key.getRightElement() == 0);
      iter = values.iterator();
      if(currKey.equals(lastKey)) {
        //If last and current key match - it will be a source-target matched pair
        // Write the values of source and target to match list
        //Write Source Paths
        keyText.set(key.getLeftElement());
        Iterator<String> sourceIter = sourceValues.listIterator();
        while(iter.hasNext()) {
          targetValue = iter.next().toString();
          matched = false;
          while(sourceIter.hasNext()) {
            sourceValue = sourceIter.next();
            if(targetValue.equals(sourceValue)) {
              //Matched pairs
              sourceIter.remove();
              matched = true;
              valueText.set(sourceValue + "\t" + targetValue);
              mos.write("match", keyText, valueText);
              break;
            }
          }
          if(!matched) {
            //Target Mismatches
            valueText.set("NO MATCHING SOURCE LOCATION\t" + targetValue);
            mos.write("mismatch", keyText, valueText);
          }
        }
        sourceIter = sourceValues.listIterator();
        while(sourceIter.hasNext()) {
          //Source Mismatches
          sourceValue = sourceIter.next();
          valueText.set(sourceValue + "\tNO MATCHING TARGET LOCATION");
          sourceIter.remove();
          mos.write("mismatch", keyText, valueText);
        }
        sourceValues.clear();
      } else {
      //If last and current key does not match
        if(currKeyBelongsToSrc) {
          // Current Key belongs to source dir
          if(lastKeyBelongsToSrc) {
            // The last source key did not have a matching target key
            // Write the values of last to key to missing list
            keyText.set(lastKey);
            for(String sourceValue : sourceValues) {
              valueText.set(sourceValue + "\tNO MATCHING TARGET");
              mos.write("missing", keyText, valueText);
            }
          }
          sourceValues.clear();
          //Add currKey's values to backup list.
          while(iter.hasNext()) {
            sourceValues.add(iter.next().toString());
          }
        } else {
          // Current Key belongs to target dir
          if(lastKeyBelongsToSrc) {
            // The last source key did not have a matching target key
            // Write the values of last to key to missing list
            keyText.set(lastKey);
            for(String sourceValue : sourceValues) {
              valueText.set(sourceValue + "\tNO MATCHING TARGET");
              mos.write("missing", keyText, valueText);
            }
          }
          sourceValues.clear();
          //The current target key did not have matching source key.
          // Write the values to mismatch list
          while(iter.hasNext()) {
            keyText.set(key.getLeftElement());
            valueText.set("NO MATCHING SOURCE\t" + iter.next());
            mos.write("missing", keyText, valueText);
          }
        }
      }
      lastKey = new String(currKey);
      lastKeyBelongsToSrc = currKeyBelongsToSrc;
      
        
    }  
    
    @Override
    protected void cleanup(Context context) 
        throws java.io.IOException ,InterruptedException {
      mos.close();
    }
  }

  private static class ComparePartitionClass extends Partitioner<PairOfStringInt, Text> {
    
    @Override
    public int getPartition(PairOfStringInt key, Text value, int numReduceTasks) {
      return 0;
    }
  }
  
  public void usage() {
    System.out.println("Compare <srcDir> <targetDir> <resultsDir> <compareBy>");
    System.out.println("\tcompareBy: Column Number");
    System.out.println("\t\t1 - Will perform the comparson by first column");
    System.out.println("\t\t2 - Will perform the comparson by second column");
    //System.out.println("\tFor path comparison, the sub-paths within srcDir and targetDir are compared.");
  }
  
  public int run(String args[]) throws Exception {
    if(args.length < 4) {
      usage();
      System.exit(-1);
    }

    Path srcPath = new Path(args[0]);
    Path targetPath = new Path(args[1]);
    Path resultsDir = new Path(args[2]);
    String compareBy = args[3];
    
    
    Job job = Job.getInstance(getConf());
    job.setJarByClass(Compare.class);
    job.setJobName("FA - Compare");
    job.getConfiguration().set("compareBy", compareBy);
    
    job.setNumReduceTasks(1);
    job.setReducerClass(CompareReducer.class);
    
    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    MultipleInputs.addInputPath(job, srcPath, TextInputFormat.class, CompareSourceMapper.class);
    MultipleInputs.addInputPath(job, targetPath, TextInputFormat.class, CompareTargetMapper.class);
    
    FileOutputFormat.setOutputPath(job, resultsDir);    

    MultipleOutputs.addNamedOutput(job, "match", TextOutputFormat.class, Text.class, Text.class);
    MultipleOutputs.addNamedOutput(job, "mismatch", TextOutputFormat.class, Text.class, Text.class);
    MultipleOutputs.addNamedOutput(job, "missing", TextOutputFormat.class, Text.class, Text.class);
    
    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Completed in " +
        ((System.currentTimeMillis() - startTime)/1000) + "secs.");
    
    return 0;
  }
  
  public static void main(String args[]) throws Exception {
    int result = ToolRunner.run(new Configuration(), new Compare(), args);
    System.exit(result);
  }
}
