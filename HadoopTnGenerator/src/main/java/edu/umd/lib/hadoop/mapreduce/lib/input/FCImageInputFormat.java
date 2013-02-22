package edu.umd.lib.hadoop.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import edu.umd.lib.hadoop.io.ImageWritable;

/**
 * An extension of {@link ImageInputFormat} that uses {@link FCImageRecordReader}.
 * 
 * @author mohideen
 *
 */

public class FCImageInputFormat extends ImageInputFormat {
  
  public RecordReader<Text, ImageWritable> createRecordReader(InputSplit split,
      TaskAttemptContext job) throws IOException, InterruptedException {
    return new FCImageRecordReader();
  }

}
