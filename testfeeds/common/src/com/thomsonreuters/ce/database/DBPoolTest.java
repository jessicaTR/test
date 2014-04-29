package com.thomsonreuters.ce.database;

import java.sql.Connection;
import java.sql.SQLException;

public class DBPoolTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		EasyConnection.configPool("cfgsamples/dbpool.conf");
		
		try {
			Connection d=new EasyConnection("red_cnr");
			d.close();
			
			Connection e=new EasyConnection("red_cnr");
			Connection f=new EasyConnection("red_cnr");
			Connection g=new EasyConnection("red_cnr");
			Connection h=new EasyConnection("red_cnr");
		
		
		
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		
	}

}
