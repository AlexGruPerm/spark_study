package gdev;

import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
//import org.apache.hive.jdbc.HiveDriver;

public class CheckHive {
	
	final static Logger logger = Logger.getLogger(CheckHive.class);
	
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
	/*
	#1
	Create hive table with parquet basic,
	CREATE TABLE prq1 (time TIMESTAMP,carnum string) STORED AS PARQUET LOCATION '/user/data/prq3_hive.parquet'; 
	
	#2
	CREATE TABLE avro_cc2 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' STORED AS AVRO TBLPROPERTIES ('avro.schema.url'='hdfs://10.242.5.88:9000/user/data/cam_car2.parquet'); 

    CREATE EXTERNAL TABLE parquet_test LIKE avro_test STORED AS PARQUET LOCATION 'hdfs://myParquetFilesPath';
	*/
	
	public CheckHive(){
	}
	
	public void run(){
		try {
		      Class.forName(driverName);
		    } catch (ClassNotFoundException e) {
		      logger.warn("run() - "+e.fillInStackTrace());
		      System.exit(1);
		    }
		
		Connection con;
		 
		try { 
			con = DriverManager.getConnection("jdbc:hive2://10.242.5.88:10000/default", "", "");//9084
	    Statement stmt = con.createStatement();
	    String tableName = "test";
	    //stmt.executeQuery("drop table " + tableName);
	    //ResultSet res = stmt.executeQuery("create table " + tableName + " (key int, value string)");
	    // show tables
	    ResultSet res = stmt.executeQuery("select * from "+tableName);
	    //String sql = "show tables '" + tableName + "'";
	    //System.out.println("Running: " + sql);
	    //res = stmt.executeQuery(sql);
	    
	      logger.info("---------------------------------------------------");
	     while (res.next()) {
	      logger.info(res.getString(1)+"  "+ res.getString(2)+"  "+ res.getString(3));
	     }
	      logger.info("---------------------------------------------------");
	    
        /*
	    String sql = "describe " + tableName;
	    System.out.println("Running: " + sql);
	    res = stmt.executeQuery(sql);
	     while (res.next()) {
	      logger.info("---> " + res.getString(1) + "\t" + res.getString(2));
	     }
	    */ 
	    
		} catch (SQLException e) {
			logger.warn(e.fillInStackTrace());
		}
		
	}

}
