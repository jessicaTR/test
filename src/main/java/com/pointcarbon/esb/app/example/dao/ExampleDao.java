package com.pointcarbon.esb.app.example.dao;

import com.pointcarbon.esb.app.example.beans.MyValue;

import oracle.jdbc.OracleTypes;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SqlOutParameter;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.core.simple.SimpleJdbcCall;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by artur on 20/03/14.
 */
@Repository
public class ExampleDao {
    private SimpleJdbcCall getMyValuesCall;

    @Autowired
    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        MyValueRowMapper myValueRowMapper = new MyValueRowMapper();

        getMyValuesCall = new SimpleJdbcCall(jdbcTemplate)
                .withoutProcedureColumnMetaDataAccess()
                .withCatalogName("pkg_my_example_package")
                .withFunctionName("get_my_values")
                .declareParameters(
                        new SqlOutParameter("return", OracleTypes.CURSOR, myValueRowMapper),
                        new SqlParameter("req_param", OracleTypes.VARCHAR)
                );
    }

    @SuppressWarnings("unchecked")
    public List<MyValue> fetchValueFromDb(String myRequestValue) {
//    	MyValue mv = new MyValue();
//    	mv.setId(1);
//    	mv.setName("user1");
//    	List<MyValue> list = new ArrayList<MyValue>();
//        
//    	return list;
    	
    	return getMyValuesCall.executeFunction(List.class, myRequestValue);
    }

    private class MyValueRowMapper implements RowMapper<MyValue> {
        @Override
        public MyValue mapRow(ResultSet resultSet, int rowNum) throws SQLException {
            MyValue myValue = new MyValue();
            myValue.setId(resultSet.getInt("val_id"));
            myValue.setName(resultSet.getString("val_name"));
            return myValue;
        }
    }
}
