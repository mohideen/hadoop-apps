package edu.umd.lib.hadoop.mapreduce.lib.input;

import java.io.IOException;
import javax.imageio.ImageIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import edu.umd.lib.hadoop.io.ImageWritable;

/**
 * An customization of {@link ImageRecordReader} to read images in the 
 * fedora repository.
 * 
 * Key: Fedora Object name
 * Value: Image content
 * 
 * @author mohideen
 *
 */

public class FCImageRecordReader extends ImageRecordReader {

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext job)
      throws IOException, InterruptedException {
    FileSplit split = (FileSplit) inputSplit;
    Configuration conf = job.getConfiguration();

    Path filePath = split.getPath();
    FileSystem fs = filePath.getFileSystem(conf);
    FSDataInputStream fileStream = fs.open(filePath);
    String pathSlices[] = filePath.toString().split("/");

    /**
     * Skip last two levels in the path to get the actual object name e.g.
     * /fedora/dsfedora/islandora_469/OBJ/OBJ.0 => islandora_469
     */
    key = new Text(pathSlices[pathSlices.length - 3]);
    value = new ImageWritable(ImageIO.read(fileStream));

  }

}