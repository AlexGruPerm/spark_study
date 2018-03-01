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
			con = DriverManager.getConnection("jdbc:hive2://10.242.5.88:9084/default", "", "");//9084
			//con = DriverManager.getConnection("jdbc:hive://10.242.5.88:9084/default", "", "");
		//Connection cnct = DriverManager.getConnection("jdbc:hive2://<host>:<port>", "<user>", "<password>");
	    Statement stmt = con.createStatement();
	    String tableName = "testHiveDriverTable";
	    stmt.executeQuery("drop table " + tableName);
	    ResultSet res = stmt.executeQuery("create table " + tableName + " (key int, value string)");
	    // show tables
	    String sql = "show tables '" + tableName + "'";
	    System.out.println("Running: " + sql);
	    res = stmt.executeQuery(sql);
	     if (res.next()) {
	      logger.info(">>> " + res.getString(1));
	     }
	    
	    // describe table
	    sql = "describe " + tableName;
	    System.out.println("Running: " + sql);
	    res = stmt.executeQuery(sql);
	     while (res.next()) {
	      logger.info("---> " + res.getString(1) + "\t" + res.getString(2));
	     }
	    
		} catch (SQLException e) {
			logger.warn(e.fillInStackTrace());
		}
		
	}

}
