package edu.umd.lib.hadoop.mapreduce.lib.input;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import edu.umd.lib.hadoop.io.ImageWritable;

/**
 * A custom input format that uses {@link ImageRecordReader} to read image
 * inputs. Reads images as {@link ImageWritable}.
 * 
 * @author mohideen
 *
 */
public class ImageInputFormat extends FileInputFormat<Text, ImageWritable> {

  @Override
  public RecordReader<Text, ImageWritable> createRecordReader(InputSplit split,
      TaskAttemptContext job) throws IOException, InterruptedException {
    return new ImageRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }
}
