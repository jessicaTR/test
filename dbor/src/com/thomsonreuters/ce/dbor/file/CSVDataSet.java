package com.thomsonreuters.ce.dbor.file;


import java.io.Reader;
import java.io.BufferedReader;
import java.io.IOException;
import au.com.bytecode.opencsv.CSVReader;

public class CSVDataSet {
	
	public String[] columnnamelist;
	public String[] valuelistofcurrentrow;
	private CSVReader thisreader;
	
	public CSVDataSet(Reader csv, char separator, char quotechar,char escape,int line, boolean strictQuotes) throws IOException
	{
		thisreader=new CSVReader(csv,separator,quotechar,escape,line, strictQuotes);		
		columnnamelist=thisreader.readNext();
		
	}
	
	public boolean next() throws IOException
	{
		valuelistofcurrentrow=thisreader.readNext();
		if (valuelistofcurrentrow==null)
		{
			return false;
		}
		return true;
	}
	
	public String getValue(int columnPos)
	{
		return valuelistofcurrentrow[columnPos];
	}
	
	public String getValue(String columnName)
	{
		for(int i=0; i<columnnamelist.length;i++)
		{
			if (columnnamelist[i].equals(columnName))
			{
				return valuelistofcurrentrow[i];
			}
		}
		return null;
	}
	
	public void close() throws IOException
	{
		this.thisreader.close();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		String row="";
		
		for(String i : valuelistofcurrentrow)
		{
			row=row+"\""+i+"\""+",";
		}
		
		return row;
	}

}
