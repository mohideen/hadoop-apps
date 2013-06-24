package edu.umd.lib.hadoopapps;


/*
 * Cloud9: A MapReduce Library for Hadoop
* 
* Licensed under the Apache License, Version 2.0 (the "License"); you
* may not use this file except in compliance with the License. You may
* obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0 
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
* implied. See the License for the specific language governing
* permissions and limitations under the License.
*/


import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
* <p>
* The output file can used as an input to 
* {@link DemoReduceSideJoin}. 
* This tool takes the following command-line arguments:
* </p>
* 
* <ul>
* <li>[input-path1] input path</li>
* </ul>
* 
* <p>
* The j. 
* </p>
* 
* 
*/
public class MD5Sum extends Configured implements Tool {
 
 private MD5Sum() {
 }
 
 private void usage() {
   System.err.println("Usage: MD5Sum filePath");
   ToolRunner.printGenericCommandUsage(System.out);
 }
 
 /**
  * Runs the app.
  */
   
 public int run (String[] args) throws Exception {
   
   if (args.length < 1) {
     usage();
     return -1;
   }
   Path filePath = new Path(args[0]);
   FileSystem hdfs = FileSystem.get(new Configuration());
   if(hdfs.exists(filePath)) {
     InputStream in = (InputStream)(hdfs.open(filePath));
     MD5Hash md5hash = MD5Hash.digest(in);
     System.out.println("MD5Sum: " + md5hash.toString());
   } else {
     System.out.println("Invalid path!");     
   }
   return 0;
 }
 
 public static void main(String[] args) throws Exception {
   int res = ToolRunner.run(new Configuration(), new MD5Sum(), args);
   System.exit(res);
 }  

}