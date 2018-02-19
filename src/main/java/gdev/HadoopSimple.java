package gdev;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import java.net.URI;

public class HadoopSimple {
	
	final static Logger logger = Logger.getLogger(HadoopSimple.class);
	
	void write_into_hdfs() throws IOException{

		String hdfsuri = "hdfs://10.242.5.88:9000";
		String path = "/user/data/";
	    String fileName = "hello.csv";
	    String fileContent = "hello;world";
		
		Configuration conf = new Configuration();

	      // Set FileSystem URI
	      conf.set("fs.defaultFS", hdfsuri);
		
		  System.setProperty("HADOOP_USER_NAME", "hadoop");
	      
	      //Get the filesystem - HDFS
	      FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);
		
	      Path workingDir=fs.getWorkingDirectory();
	      Path newFolderPath= new Path(path);
	      if(!fs.exists(newFolderPath)) {
	         // Create new Directory
	         fs.mkdirs(newFolderPath);
	         logger.info(">>>>>>  Path "+path+" created.");
	      }
	      
	      //==== Write file
	      logger.info("Begin Write file into hdfs");
	      //Create a path
	      Path hdfswritepath = new Path(newFolderPath + "/" + fileName);
	      //Inpit output stream
	      FSDataOutputStream outputStream=fs.create(hdfswritepath);
	      //Classical output stream usage
	      outputStream.writeBytes(fileContent);
	      outputStream.close();
	      logger.info("End Write file into hdfs");
		
	      //==== Read file
	      logger.info("Read file into hdfs");
	      //Create a path
	      Path hdfsreadpath = new Path(newFolderPath + "/" + fileName);
	      //Init input stream
	      FSDataInputStream inputStream = fs.open(hdfsreadpath);
	      //Classical input stream usage
	      String out= IOUtils.toString(inputStream, "UTF-8");
	      logger.info("---------- >>> ["+out+"]");
	      inputStream.close();
	      fs.close();
		
	}
}
