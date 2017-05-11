package org.light32.dynsql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * this class will convert JDBC ResultSets into HashMaps for access to queried data without
 * wasting time with ORM.
 * 
 * This class will maintain a cache of results for each resultSet processed.  Be careful with this.
 * 
 * 
 * @author jwhitt
 *
 */
public class ResultSetHandler {

	public static final int ALL_ROWS = -1;
	private final int rowsInList;
	private ResultSet contextResultSet;
	private List<Map<String, String>> contextResultCache;
	
	/**
	 * old school constructors for use as an object
	 * 
	 * @param blockLength
	 * @param contextResultSet
	 */
	public ResultSetHandler(int blockLength, ResultSet contextResultSet) { 
		this.rowsInList = blockLength;
		this.contextResultSet = contextResultSet;
	}	
	public ResultSetHandler() { 
		this(ALL_ROWS, null);
	}
	public ResultSetHandler(ResultSet rs) { 
		this(ALL_ROWS, rs);
	}
	
	/**
	 * fluent constructor, e.g.
	 * 
	 * ResultSetHandler.with(resultSet).doList();
	 * or
	 * ResultSetHandler.with(resultSet)
	 * 				.stream()
	 * 				.map()
	 * 				.reduce()
	 * 				.lambda()
	 * 				.goodness()
	 * 
	 * @param rs
	 * @return ResultSetHandler
	 */
	public static ResultSetHandler with(ResultSet rs) { 
		return new ResultSetHandler(rs);
		
	}
	
	/**
	 * returns the contents of a ResultSet in a list of Maps.
	 * This is intended for volume queries in order to remove the overhead of 
	 * setting up ORM for applications that do a lot of selects and very little DML.
	 * 
	 * @param rs
	 * @return List<Map<String, String>>
	 */
	public List<Map<String, String>> doList(ResultSet rs) throws SQLException { 
		// null ResultSet means nothing to do
		if (rs == null) { 
			return null; 
		}
		
		// if the contextResultSet was passed in, return cached results if not empty.
		if(rs.equals(contextResultSet)) { 
			return (contextResultCache == null || contextResultCache.isEmpty()) ? null : contextResultCache;
		}
		
		// we have a new result set so stuff to do
		// generate map of column name -> position for optimization
		ResultSetMetaData md = rs.getMetaData();
		int colCount = md.getColumnCount();
		
		contextResultCache = new ArrayList<>();
		
		// init hashmap with exact column count and 1:1 load factor
		Map<String, Integer> colmap = new HashMap<String, Integer>(colCount, 1);
		
		int i = 1;
		while (md.getColumnCount() <= i) { 
			colmap.put(md.getColumnLabel(i),i);
		}

		// loop through rows
		// TODO implement batching and row count/restart on next call
		// might be worth implementing Iterable?
		while (rs.next()) {			
			// exact width/no load again
			Map<String, String> row = new HashMap<String, String>(colCount,1);
			
			for(Map.Entry<String, Integer> colmapEntry : colmap.entrySet()) { 
				row.put(colmapEntry.getKey(), rs.getString(colmapEntry.getValue()));
			}
			
			contextResultCache.add(Collections.unmodifiableMap(row));
		}
		
		// note: do not close result set.  managing the result set is the caller's responsibility.
		// return null if nothing in the output
		return (contextResultCache.isEmpty()) ? null : contextResultCache;
	}
	
	/**
	 * implicitly processes the contextResultSet
	 * 
	 * @return
	 * @throws SQLException
	 */
	public List<Map<String, String>> doList() throws SQLException { 
		return this.doList(contextResultSet);
	}
	
	/**
	 * convenience method to get stream
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	public Stream<Map<String, String>> stream(ResultSet rs) throws SQLException { 
		return doList(rs).stream();
	}
	
	/**
	 * implicitly streams the contextResultSet
	 * @return
	 * @throws SQLException
	 */
	public Stream<Map<String, String>> stream() throws SQLException { 
		return stream(contextResultSet);
	}
}
