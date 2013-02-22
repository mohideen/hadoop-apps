package edu.umd.lib.hadoop.mapreduce.lib.input;

import java.io.IOException;
import javax.imageio.ImageIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import edu.umd.lib.hadoop.io.ImageWritable;

/**
 * A custom record reader that uses reads each individual image file
 * as a single record.
 * 
 * Key: Image File Name
 * Value: Image Content (as {@link ImageWritable})
 * 
 * @author mohideen
 *
 */
public class ImageRecordReader extends RecordReader<Text, ImageWritable> {
  Text key = null;
  ImageWritable value = null;
  Boolean recordRead = false;
  FSDataInputStream fileStream;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext job)
      throws IOException, InterruptedException {
    FileSplit split = (FileSplit) inputSplit;
    Configuration conf = job.getConfiguration();

    Path filePath = split.getPath();
    FileSystem fs = filePath.getFileSystem(conf);
    fileStream = fs.open(filePath);

    key = new Text(filePath.getName());
    value = new ImageWritable(ImageIO.read(fileStream));
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!recordRead) {
      recordRead = true;
      return true;
    }
    return false;
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public ImageWritable getCurrentValue() throws IOException,
      InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (key == null) {
      return 0;
    } else {
      return 100;
    }
  }

  @Override
  public void close() throws IOException {
    if (fileStream != null) {
      fileStream.close();
    }
  }
}