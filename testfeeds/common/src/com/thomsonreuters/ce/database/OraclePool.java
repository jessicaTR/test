package com.thomsonreuters.ce.database;

import java.sql.*;
import oracle.ucp.jdbc.*;
import oracle.ucp.admin.*; 
import oracle.ucp.*;

//import org.apache.log4j.Logger;

public class OraclePool {

	private PoolDataSource pds = null;
	private UniversalConnectionPool pool = null;
	private String CACHE_NAME;
	private int MaxCachedSize;
	private int WaitNum=0;

	/**
	 * This method initializes Cache.
	 **/
	public OraclePool(String DB_CACHE_NAME,String DB_Url, String User_Name,
			String Pass_Word, int init_Size, int min_Size,
			int max_Size) {

		try {

			this.CACHE_NAME=DB_CACHE_NAME;

			pds = PoolDataSourceFactory.getPoolDataSource(); 
			pds.setURL(DB_Url);
			pds.setUser(User_Name);
			pds.setPassword(Pass_Word);
			pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource"); 
			pds.setConnectionPoolName(this.CACHE_NAME); 

			pds.setMinPoolSize(min_Size); 
			pds.setMaxPoolSize(max_Size); 
			pds.setInitialPoolSize(init_Size); 
			//pds.setInactiveConnectionTimeout(60); 

			UniversalConnectionPoolManager connMgr = UniversalConnectionPoolManagerImpl.getUniversalConnectionPoolManager();;
			connMgr.createConnectionPool((UniversalConnectionPoolAdapter)pds);
			connMgr.startConnectionPool(CACHE_NAME);
			pool=connMgr.getConnectionPool(CACHE_NAME); 
			MaxCachedSize=max_Size;

		}
		catch (Exception ex) {
			throw new DBException(ex);
		}
	}

	/**
	 * @return Database Connection Object
	 */
	protected Connection GetConnection() {

		try {

			Connection Dbconn = null;

			synchronized (this) {

				UniversalConnectionPoolStatistics stats = pool.getStatistics(); 
				int usedCount = stats.getBorrowedConnectionsCount(); 


				while (usedCount == this.MaxCachedSize) {
					try {
						WaitNum++;
						this.wait();
						WaitNum--;
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

				Dbconn = pds.getConnection();

			}

			Dbconn.setAutoCommit(false);
			return Dbconn;

		} catch (Exception ex) {
			throw new DBException(ex);
		}

	}

	protected void CloseConnection(Connection DBConn) {
		try {

			synchronized (this) {
				DBConn.close();
				if (WaitNum>0)
				{
					this.notify();
				}
			}

		}
		catch (SQLException ex) {
			throw new DBException(ex);
		}
	}

	/**
	 * This method returns active size of the Cache.
	 **/
	public int GetActiveSize() {
		try {

			UniversalConnectionPoolStatistics stats = pool.getStatistics(); 
			int usedCount = stats.getBorrowedConnectionsCount(); 
			return usedCount;
		}
		catch (Exception ex) {
			throw new DBException(ex);
		}
	}

	/**
	 * This method returns connection cache size.
	 **/
	public int GetCacheSize() {

		try {

			UniversalConnectionPoolStatistics stats = pool.getStatistics(); 
			int usedCount = stats.getBorrowedConnectionsCount(); 
			int AvailableCount = stats.getAvailableConnectionsCount(); 

			return usedCount+AvailableCount;
		}
		catch (Exception ex) {
			throw new DBException(ex);
		}
	}

	/**
	 * This method returns free connection cache size.
	 **/
	public int getAvailableSize() {

		try {
			UniversalConnectionPoolStatistics stats = pool.getStatistics(); 
			int AvailableCount = stats.getAvailableConnectionsCount(); 

			return AvailableCount;
		}
		catch (Exception ex) {
			throw new DBException(ex);
		}
	}

	/**
	 *  This method closes the connection cache.
	 **/
	public void close() {
		try {			
			UniversalConnectionPoolManager connMgr = UniversalConnectionPoolManagerImpl.getUniversalConnectionPoolManager();
			connMgr.stopConnectionPool(CACHE_NAME);
			connMgr.destroyConnectionPool(CACHE_NAME);
		}
		catch (Exception ex) {
			throw new DBException(ex);
		}
	}
}
