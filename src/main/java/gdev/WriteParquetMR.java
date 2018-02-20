package gdev;

import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MILLIS;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.Preconditions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

public class WriteParquetMR {
	
	final static Logger logger = Logger.getLogger(WriteParquetMR.class);
	
	void write_simple() throws Exception {

		String hdfsuri = "hdfs://192.168.1.14:8020";
		String path = "/user/data/";
	    String fileName = "cam_car.parquet";
	
		 try{
		  	    MessageType CAM_CAR_SNAP_FILE_SCHEMA = Types.buildMessage()
		  		      .required(INT64).as(TIMESTAMP_MILLIS).named("time")
		  		      .required(BINARY).as(UTF8).named("carnum")
		  		      .named("cam_car");

		  	    SimpleGroupFactory GROUP_FACTORY_CAM_CAR_SNAP = new SimpleGroupFactory(CAM_CAR_SNAP_FILE_SCHEMA);

		  	    Configuration conf = new Configuration();
		  	    
		  	    System.setProperty("HADOOP_USER_NAME", "root");

			    Path hdfswritepath = new Path(hdfsuri+path + fileName);
			    ParquetWriter<Group> cc_writer = ExampleParquetWriter.builder(hdfswritepath)
			        .withType(CAM_CAR_SNAP_FILE_SCHEMA)
			        .build();
			    
			    Long start_ts = (long) 1505407820; 
			    for (int i=0;i<100000;i++){
			    	Group group_cc = GROUP_FACTORY_CAM_CAR_SNAP.newGroup(); 
			    	start_ts = start_ts+1;
			    	String car_num = RandomStringUtils.randomAlphanumeric(4).toUpperCase();
			    	group_cc.add("time",    start_ts*1000);
			    	group_cc.add("carnum",  car_num   );
        		    cc_writer.write(group_cc);	
			    }
			    cc_writer.close(); 
		    } catch (IOException e) {
				logger.debug(e.fillInStackTrace());
			}
	
	}
	


}
