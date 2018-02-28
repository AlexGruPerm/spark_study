package gdev;

import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MILLIS;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

import java.io.IOException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
 

public class WriteParquetMR { 
	
	final static Logger logger = Logger.getLogger(WriteParquetMR.class);

	void write_simple() throws Exception {
 
		String hdfsuri = "hdfs://10.242.5.88:9000"; // "hdfs://192.168.1.14:8020";
		String path = "/user/data/";
	    String fileName = "cam_car2.parquet";

		 try{
		  	    MessageType CAM_CAR_SNAP_FILE_SCHEMA = Types.buildMessage()
		  		      .required(INT64).as(TIMESTAMP_MILLIS).named("time") // INT64
		  		      .required(BINARY).as(UTF8).named("carnum")
		  		      .named("cam_car");

		  	    SimpleGroupFactory GROUP_FACTORY_CAM_CAR_SNAP = new SimpleGroupFactory(CAM_CAR_SNAP_FILE_SCHEMA);

		  	    Configuration conf = new Configuration();
		  	    
		  	    System.setProperty("HADOOP_USER_NAME", "hadoop"); //root

			    Path hdfswritepath = new Path(hdfsuri + path + fileName);
 
			    conf.set("dfs.blocksize", "67108864");
			    conf.set("dfs.replication", "2");

			    ParquetWriter<Group> cc_writer = GdevParquetWriter.builder(hdfswritepath)
				        .withType(CAM_CAR_SNAP_FILE_SCHEMA)
				        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)  
				        .withDictionaryEncoding(false)
				        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
				        .withRowGroupSize(67108864)
				        .withConf(conf)
				        .build();

			    Long start_ts = (long) 1505407820; 
			    logger.info(" Begin write parquet");
			    for (Long i=(long)0; i<1000L/*1000000000L*/; i++){
			    	Group group_cc = GROUP_FACTORY_CAM_CAR_SNAP.newGroup(); 
			    	start_ts = start_ts+1;
			    	String car_num = RandomStringUtils.randomAlphanumeric(4).toUpperCase();
			    	group_cc.add("time",    start_ts*1000);
			    	group_cc.add("carnum",  car_num   );
        		    cc_writer.write(group_cc);	
			    }
			    cc_writer.close(); 
			    logger.info(" End write parquet");
		    } catch (IOException e) {
				logger.warn(e.fillInStackTrace());
			}

		 debug_ds(hdfsuri + path + fileName);
	
	}
	
	public void debug_ds(String apploaded_prq_fpath){
		logger.info(" >>> debug_ds ");
		SparkSession spark = SparkSession.builder()
				.appName("Java Spark SQL basic example")
				.config("spark.master", "local[*]")
				.config("spark.sql.crossJoin.enabled", "true").getOrCreate();

		Dataset<Row> sqlLoadedDF = spark.read().load(apploaded_prq_fpath);

		sqlLoadedDF.createOrReplaceTempView("v_cc");
/*
		Dataset<Row> usrs_CntApplod_WeekReg = spark
				.sql("SELECT v_aload.time,  current_timestamp as cts, datediff(current_timestamp,v_aload.time) as dd, date_add(v_aload.time,7) as t7 FROM v_aload ");
*/
		//Dataset<Row> usrs_CntApplod_WeekReg = spark.sql("SELECT min(time) as begin_dt, max(time) as end_dt, count(*) as CNT FROM v_cc ");

		Dataset<Row> usrs_CntApplod_WeekReg = spark.sql("SELECT * FROM v_cc "); 
		
		usrs_CntApplod_WeekReg.show();  
		
		logger.info(" <<< debug_ds ");
}
	


}
