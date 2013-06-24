package edu.umd.lib.hadoopapps;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
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
  
  private Path createMapInputs(Path sourcePath, int numMaps) throws Exception{
    FileSystem hdfs = FileSystem.get(getConf());
    String pathPrefix = "/tmp/md5batch/" + System.currentTimeMillis() + sourcePath.getName();
    Path listPath = new Path(pathPrefix + "-list.seq");
    FileListing fileListing = new FileListing();
    Path mapInputDir;
    try {
      long numPaths = fileListing.createListing(sourcePath, listPath);
      mapInputDir = new Path(pathPrefix + "-inputdir");
      hdfs.mkdirs(mapInputDir);
      SequenceFile.Reader.Option pathOptR = SequenceFile.Reader.file(listPath);
      SequenceFile.Reader sReader = new SequenceFile.Reader(getConf(), pathOptR);
      
      SequenceFile.Writer.Option pathOpt;
      SequenceFile.Writer.Option keyClassOpt = SequenceFile.Writer.keyClass(Text.class);
      SequenceFile.Writer.Option valClassOpt = SequenceFile.Writer.valueClass(NullWritable.class);
      SequenceFile.Writer.Option optCom = SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE);
      Text pathString = new Text();
      NullWritable nWritable = NullWritable.get();
      Path tmpInputFilePath;
      SequenceFile.Writer seqFileWriter;
      if(numMaps > numPaths) {
        for(int count=0; count<numPaths; count++) {
          tmpInputFilePath = new Path(mapInputDir.toString() + "/part-" + count);
          pathOpt = SequenceFile.Writer.file(tmpInputFilePath);
          seqFileWriter = SequenceFile.createWriter(getConf(), pathOpt, keyClassOpt, 
              valClassOpt, optCom);
          sReader.next(pathString);
          seqFileWriter.append(pathString, nWritable);
        }
      } else {
        long addedPaths = 0;
        for(int count=1; count<=numMaps; count++) {
          tmpInputFilePath = new Path(mapInputDir.toString() + "/part-" + count);
          pathOpt = SequenceFile.Writer.file(tmpInputFilePath);
          seqFileWriter = SequenceFile.createWriter(getConf(), pathOpt, keyClassOpt, 
              valClassOpt, optCom);
          while(addedPaths < ((numPaths/numMaps)*count)) {
            sReader.next(pathString);
            seqFileWriter.append(pathString, nWritable);
            addedPaths++;
          }
                  
          if(count == numMaps) {
            while(sReader.next(pathString)) {
              seqFileWriter.append(pathString, nWritable);
            }
          }
          seqFileWriter.close();        
        }
      }
    } finally {
      hdfs.delete(listPath, false);
    }
    return mapInputDir;
    
  }

  public int run(String[] args) throws Exception {
    
    if(args.length < 3) {
      usage();
      System.exit(-1);
    }
    FileSystem hdfs = FileSystem.get(getConf());
    String inputFile = args[0];
    String outputDir = args[1];
    String numMaps = args[2];

    Path inputPath = new Path(inputFile);
    Path outputPath = new Path(outputDir);
    Path mapInputDir;
    mapInputDir = createMapInputs(inputPath, Integer.parseInt(numMaps));
    
      
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
    SequenceFileInputFormat.addInputPath(job, mapInputDir);
    FileOutputFormat.setOutputPath(job, outputPath);
    
    try { 
      long startTime = System.currentTimeMillis();
      job.waitForCompletion(true);
      System.out.println("Completed in " +
          ((System.currentTimeMillis() - startTime)/1000) + "secs.");
    } finally {
      if(mapInputDir != null) {
        hdfs.delete(mapInputDir, true);
      }
    }
    
    return 0;
    
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MD5Batch(), args);
    System.exit(res);
  }
  
}
