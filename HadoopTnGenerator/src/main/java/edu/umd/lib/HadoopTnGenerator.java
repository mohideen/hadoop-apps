package edu.umd.lib;

import java.awt.image.BufferedImage;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.imgscalr.Scalr;

import edu.umd.lib.hadoop.io.ImageWritable;
import edu.umd.lib.hadoop.mapreduce.lib.input.FCImageInputFormat;
import edu.umd.lib.hadoop.mapreduce.lib.output.ImageOutputFormat;

/**
 * Hadoop Thumbnail Generator
 * 
 * A simple app that generates thumbnails of images. The app reads the input
 * images from HDFS, and writes the generated thumbnails back to HDFS. ImgScalr
 * library is used for resizing the image.
 * 
 * @author mohideen
 * 
 */
public class HadoopTnGenerator extends Configured implements Tool {

  public static final Logger logger = Logger.getLogger(HadoopTnGenerator.class);

  public static class MapClass extends
      Mapper<Text, ImageWritable, Text, ImageWritable> {

    int thumbnailSize;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      thumbnailSize = Integer.parseInt(conf.get("tn.size"));
    }

    // Map Function - Resizes the image
    @Override
    public void map(Text key, ImageWritable value, Context context)
        throws IOException, InterruptedException {
      BufferedImage tmpImage = Scalr.resize(value.buffer, thumbnailSize);
      ImageWritable imageBuffer = new ImageWritable(tmpImage);
      context.write(key, imageBuffer);
    }

  }

  public int run(String[] args) throws Exception {
    boolean result;

    // Configure the job parameters
    Configuration conf = getConf();
    conf.set("mapred.child.java.opts", "-Xmx1024m");
    conf.set("tn.size", args[0]);
    Job job = new Job(conf, "Thumbnail Generator");
    job.setJarByClass(HadoopTnGenerator.class);
    job.setNumReduceTasks(0);
    job.setMapperClass(MapClass.class);
    job.setInputFormatClass(FCImageInputFormat.class);
    job.setOutputFormatClass(ImageOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(ImageWritable.class);

    Path inputPath1 = new Path("/fedora/dsfedora/islandora_469/OBJ/OBJ.0");
    Path inputPath2 = new Path("/fedora/dsfedora/islandora_472/OBJ/OBJ.0");
    Path inputPath3 = new Path("/fedora/dsfedora/islandora_475/OBJ/OBJ.0");
    Path outputDir = new Path("/thumbnails");

    // Add inputs to the job
    FCImageInputFormat.addInputPath(job, inputPath1);
    FCImageInputFormat.addInputPath(job, inputPath2);
    FCImageInputFormat.addInputPath(job, inputPath3);

    // Set the job output dir
    ImageOutputFormat.setOutputPath(job, outputDir);

    long startTime = System.nanoTime();
    // Start the job, and wait for completion
    result = job.waitForCompletion(true);
    logger.info("Completed in" + (System.nanoTime() - startTime) / 1000000000
        + "secs.");

    return result ? 0 : -1;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new Configuration(), new HadoopTnGenerator(),
        args);
    System.exit(result);
  }

}
