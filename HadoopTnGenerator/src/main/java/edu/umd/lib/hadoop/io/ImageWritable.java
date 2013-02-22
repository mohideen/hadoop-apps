
package edu.umd.lib.hadoop.io;

import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.imageio.ImageIO;
import org.apache.hadoop.io.Writable;

/**
 * A writable interface implementation for images
 * 
 * @author mohideen
 *
 */
public class ImageWritable implements Writable {
	
	public BufferedImage buffer;
	
	public ImageWritable(BufferedImage buffer) {
		this.buffer = buffer;
	}

	public void readFields(DataInput in) throws IOException {
		buffer = ImageIO.read(new BufferedInputStream((InputStream)in));
	}

	public void write(DataOutput out) throws IOException {
		ImageIO.write(buffer, "png", (OutputStream)out);

	}

}
