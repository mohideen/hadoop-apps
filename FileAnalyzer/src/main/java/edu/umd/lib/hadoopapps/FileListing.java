package edu.umd.lib.hadoopapps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FileListing extends Configured implements Tool {
  
  private void usage() {
    System.err.println("Usage: listpaths sourceDir outputPath ");
    ToolRunner.printGenericCommandUsage(System.out);
  }
  
  public long createListing(Path sourcePath, Path outputPath)  throws Exception {
    Configuration conf = new Configuration();
    FileSystem hdfs = FileSystem.get(conf);
    hdfs.create(outputPath, true);
    long numPaths = 0;
    SequenceFile.Writer.Option pathOpt = SequenceFile.Writer.file(outputPath);
    SequenceFile.Writer.Option keyClassOpt = SequenceFile.Writer.keyClass(Text.class);
    SequenceFile.Writer.Option valClassOpt = SequenceFile.Writer.valueClass(NullWritable.class);
    SequenceFile.Writer.Option optCom = SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE);
    
    if(hdfs.exists(sourcePath)) {
      
      SequenceFile.Writer outputList = SequenceFile.createWriter(conf, pathOpt, keyClassOpt, 
          valClassOpt, optCom);
      Text pathString = new Text();
      NullWritable nWritable = NullWritable.get();
      if(hdfs.isDirectory(sourcePath)) {
        RemoteIterator<LocatedFileStatus> pathIterator = (RemoteIterator<LocatedFileStatus>)hdfs.listFiles(sourcePath, true);
        FileStatus currentFile;
        while(pathIterator.hasNext()) {
          currentFile = (FileStatus)pathIterator.next();
          pathString.set(currentFile.getPath().toString());
          System.out.println(currentFile.getPath().toString());
          outputList.append(pathString, nWritable);
          numPaths++;
        }
        outputList.close();
        
        /*
        SequenceFile.Reader.Option pathOptR = SequenceFile.Reader.file(outputPath);
        SequenceFile.Reader sReader = new SequenceFile.Reader(getConf(), pathOptR);
        while(sReader.next(pathString)) {
          System.out.println(pathString.toString());
        }*/
        
      } else {
        System.out.println("Source path is not a directory!");    
      }

    } else {
      System.out.println("Invalid source path!");     
    }
    return numPaths;
  }
  
  /**
   * Runs the app.
   */
    
  public int run (String[] args) throws Exception {
    
    if (args.length < 2) {
      usage();
      return -1;
    }
    
    Path sourcePath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    createListing(sourcePath, outputPath);
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new FileListing(), args);
    System.exit(res);
  }  

 }
