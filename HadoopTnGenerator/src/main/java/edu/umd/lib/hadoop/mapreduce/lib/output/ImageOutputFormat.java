package edu.umd.lib.hadoop.mapreduce.lib.output;

import java.io.IOException;
import java.io.OutputStream;
import javax.imageio.ImageIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import edu.umd.lib.hadoop.io.ImageWritable;

/**
 * An output format that writes {@link ImageWritable} records as png images.
 * 
 * Can be customized to check for required image output format from 
 * the job configuration.
 * 
 * @author mohideen
 *
 */
public class ImageOutputFormat extends FileOutputFormat<Text, ImageWritable> {

  public class ImageRecordWriter extends RecordWriter<Text, ImageWritable> {

    OutputStream out;
    TaskAttemptContext job;
    FileSystem fs;
    FSDataOutputStream fileStream;

    public ImageRecordWriter(TaskAttemptContext job) {
      this.job = job;
    }

    @Override
    public void write(Text key, ImageWritable value) throws IOException,
        InterruptedException {
      Path outputFilePath = getDefaultWorkFile(job, "");
      String pathString = outputFilePath.toString();
      int endOfDir = pathString.lastIndexOf("/");
      String outputDir = pathString.substring(0, endOfDir);
      Path newOutputPath = new Path(outputDir + "/" + key.toString() + ".png");
      Configuration conf = job.getConfiguration();
      fs = newOutputPath.getFileSystem(conf);
      fileStream = fs.create(newOutputPath, false);
      ImageIO.write(value.buffer, "png", fileStream);
      fileStream.flush();
    }

    @Override
    public void close(TaskAttemptContext job) throws IOException,
        InterruptedException {
      if (fileStream != null) {
        fileStream.close();
      }
    }
  }

  @Override
  public RecordWriter<Text, ImageWritable> getRecordWriter(
      TaskAttemptContext job) throws IOException, InterruptedException {
    return new ImageRecordWriter(job);
  }

}
