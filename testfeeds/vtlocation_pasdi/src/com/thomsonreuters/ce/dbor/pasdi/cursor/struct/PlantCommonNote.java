package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.io.Reader;
import java.io.IOException;
import java.util.Date;



import com.thomsonreuters.ce.dbor.pasdi.SDIConstants;
public class PlantCommonNote extends SDICursorRow {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String plant_note_title;
	private Date plant_note_date;
	private String plant_note_data_source;
	private String plant_note_text;
	private String plant_story_code;
	
	
	public PlantCommonNote(long Perm_ID, String plant_note_title, Date plant_note_date, String plant_note_data_source, String plant_note_text, String plant_story_code) {
		super(Perm_ID);
		this.plant_note_title = plant_note_title;
		this.plant_note_date = plant_note_date;
		this.plant_note_data_source = plant_note_data_source;
		this.plant_note_text = plant_note_text;
		this.plant_story_code = plant_story_code;
	}

	public static PlantCommonNote LoadDataFromRS(ResultSet RS) throws SQLException
	{
		long Perm_ID= RS.getLong("perm_id");
		String plant_note_title=RS.getString("plant_note_title");
		Date plant_note_date=null;
		Timestamp Ts=RS.getTimestamp("plant_note_date");
		if (Ts!=null)
		{
			plant_note_date = new Date(Ts.getTime());
		}
		String plant_note_data_source=RS.getString("plant_note_data_source");
		
		String plant_note_text=null;
		java.sql.Clob c=RS.getClob("plant_note_text");
		
		if (c!=null)
		{
			Reader a = c.getCharacterStream(); 

			try {
				char[] buff2 = new char[1024]; 
				plant_note_text="";
				
				int readCount=0;
				while ((readCount=a.read(buff2))>0) 
				{
					plant_note_text = plant_note_text.concat(new String (buff2,0,readCount)); 
				}
				
			} catch (IOException e) {
				SDIConstants.SDILogger.error("IOException",e);
			}
		}	
		String plant_story_code=RS.getString("plant_story_code");
		
		return new PlantCommonNote(Perm_ID, plant_note_title, plant_note_date, plant_note_data_source, plant_note_text, plant_story_code);
	}

	public String getPlant_note_data_source() {
		return plant_note_data_source;
	}


	public Date getPlant_note_date() {
		return plant_note_date;
	}


	public String getPlant_note_text() {
		return plant_note_text;
	}


	public String getPlant_note_title() {
		return plant_note_title;
	}


	public String getPlant_story_code() {
		return plant_story_code;
	}
	
	
	


}
