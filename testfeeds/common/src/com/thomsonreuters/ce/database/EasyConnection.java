package com.thomsonreuters.ce.database;

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.SQLClientInfoException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Struct;
import java.sql.SQLXML;
import java.util.*;
import java.util.concurrent.Executor;
import java.io.*;
import oracle.ucp.jdbc.ValidConnection;


public class EasyConnection implements Connection{

	private static Hashtable<String, OraclePool> DBPoolList = new Hashtable<String, OraclePool>();

	private Connection m_delegate = null;

	private String DBName = null;

	public static void configPool(String Config_File) {
		try {
			FileInputStream fis = new FileInputStream(Config_File);
			Properties prop = new Properties();
			prop.load(fis);

			StringTokenizer dbnameList = new StringTokenizer(prop
					.getProperty("database_name"), ",", false);
			while (dbnameList.hasMoreTokens()) {
				String DbName = dbnameList.nextToken().trim();
				String DB_Url = prop.getProperty(DbName + ".dburl");
				String User_Name = prop.getProperty(DbName + ".user");
				String Pass_Word = prop.getProperty(DbName + ".password");
				int init_Size = Integer.parseInt(prop.getProperty(DbName
						+ ".initsize"));
				int min_size = Integer.parseInt(prop.getProperty(DbName
						+ ".minsize"));
				int max_Size = Integer.parseInt(prop.getProperty(DbName
						+ ".maxsize"));
				DBPoolList.put(DbName, new OraclePool(DbName, DB_Url,
						User_Name, Pass_Word, init_Size, min_size, max_Size));
			}
		} catch (FileNotFoundException ex) {
			throw new DBException(ex);
		} catch (IOException ex) {
			throw new DBException(ex);
		}
	}

	public static void CloseAllPool() {
		Enumeration<OraclePool> PoolList = DBPoolList.elements();
		while (PoolList.hasMoreElements()) {
			OraclePool thisPool = PoolList.nextElement();
			thisPool.close();
		}
	}

	public EasyConnection(String db_name) {
		this.DBName = db_name;
		
		OraclePool op=DBPoolList.get(this.DBName);
		m_delegate = op.GetConnection();
		
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.isWrapperFor(iface);
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.unwrap(iface);
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		// TODO Auto-generated method stub
		m_delegate.abort(executor);
	}

	@Override
	public void clearWarnings() throws SQLException {
		// TODO Auto-generated method stub
		m_delegate.clearWarnings();
	}

	@Override
	public void close() throws SQLException {
		// TODO Auto-generated method stub
		try {
			
			m_delegate.rollback();
			OraclePool op=DBPoolList.get(this.DBName);
			op.CloseConnection(m_delegate);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			((ValidConnection)m_delegate).setInvalid();
		}
	}

	@Override
	public void commit() throws SQLException {
		// TODO Auto-generated method stub
		m_delegate.commit();
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.createArrayOf(typeName,elements);
	}

	@Override
	public Blob createBlob() throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.createBlob();
	}

	@Override
	public Clob createClob() throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.createClob();
	}

	@Override
	public NClob createNClob() throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.createNClob();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.createSQLXML();
	}

	@Override
	public Statement createStatement() throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.createStatement();
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.createStatement(resultSetType,resultSetConcurrency);
	}

	@Override
	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.createStatement(resultSetType,resultSetConcurrency,resultSetHoldability);
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.createStruct(typeName,attributes);
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.getAutoCommit();
	}

	@Override
	public String getCatalog() throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.getCatalog();
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.getClientInfo();
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.getClientInfo(name);
	}

	@Override
	public int getHoldability() throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.getHoldability();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.getMetaData();
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.getNetworkTimeout();
	}

	@Override
	public String getSchema() throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.getSchema();
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.getTransactionIsolation();
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.getTypeMap();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.getWarnings();
	}

	@Override
	public boolean isClosed() throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.isClosed();
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.isReadOnly();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.isValid(timeout);
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.nativeSQL(sql);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.prepareCall(sql);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.prepareCall(sql,resultSetType,resultSetConcurrency);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.prepareCall(sql,resultSetType,resultSetConcurrency,resultSetHoldability);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.prepareStatement(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.prepareStatement(sql,autoGeneratedKeys);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.prepareStatement(sql,columnIndexes);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.prepareStatement(sql,columnNames);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.prepareStatement(sql,resultSetType,resultSetConcurrency);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.prepareStatement(sql,resultSetType,resultSetConcurrency,resultSetHoldability);
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		// TODO Auto-generated method stub
		m_delegate.releaseSavepoint(savepoint);
	}

	@Override
	public void rollback() throws SQLException {
		// TODO Auto-generated method stub
		m_delegate.rollback();
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		// TODO Auto-generated method stub
		m_delegate.rollback(savepoint);
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		// TODO Auto-generated method stub
		m_delegate.setAutoCommit(autoCommit);
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		// TODO Auto-generated method stub
		m_delegate.setCatalog(catalog);
	}

	@Override
	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		// TODO Auto-generated method stub
		m_delegate.setClientInfo(properties);
	}

	@Override
	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		// TODO Auto-generated method stub
		m_delegate.setClientInfo(name,value);
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		// TODO Auto-generated method stub
		m_delegate.setHoldability(holdability);
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
		// TODO Auto-generated method stub
		m_delegate.setNetworkTimeout(executor,milliseconds);
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		// TODO Auto-generated method stub
		m_delegate.setReadOnly(readOnly);
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.setSavepoint();
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		// TODO Auto-generated method stub
		return m_delegate.setSavepoint(name);
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		// TODO Auto-generated method stub
		m_delegate.setSchema(schema);
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		// TODO Auto-generated method stub
		m_delegate.setTransactionIsolation(level);
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		// TODO Auto-generated method stub
		m_delegate.setTypeMap(map);
	}


}
