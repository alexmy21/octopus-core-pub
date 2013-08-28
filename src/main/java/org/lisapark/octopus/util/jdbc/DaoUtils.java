/* 
 * Copyright (C) 2013 Lisa Park, Inc. (www.lisa-park.net)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.lisapark.octopus.util.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class DaoUtils {
    
    public static final String SQL_FIELDS_STRING    = "sqlFieldsString";
    public static final String SQL_VALUES_STRING    = "sqlValuesString";
   
    /**
     * 
     * @param query
     * @param identField, set to null if do not use generated keys
     * @return 
     */
    public static synchronized int insert(String query, String identField, Connection conn) throws SQLException {
        
        int key = 0;
       
//        try {
            Statement stmt = conn.createStatement();            
            
            if (identField != null) {
                stmt.executeUpdate(query, new String[]{identField});
                ResultSet rs = stmt.getGeneratedKeys();
                if (rs.next()) {
                    key = rs.getInt(1); //identField);
                }
            } else {
                stmt.executeUpdate(query);
            }
//            
//        } catch (SQLException ex) {
//            Logger.getLogger(DaoUtils.class.getName()).log(Level.SEVERE, null, ex);
//        } 
        
        return key;
    }
    
    /**
     * 
     * @param tblMap
     * @param tblName
     * @param identField, set to null if do not use generated keys
     * @return 
     */
    public static synchronized String insertQueryString(Map<String, Object> tblMap, String tblName, String identField) {
        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO ").append(tblName)
                .append("(")
                .append(sqlInsertStrings(tblMap).get(DaoUtils.SQL_FIELDS_STRING))
                .append(") VALUES (")
                .append(sqlInsertStrings(tblMap).get(DaoUtils.SQL_VALUES_STRING))
                .append(")");
        
//        Logger.getLogger(DaoUtils.class.getName()).log(Level.INFO, query.toString(), query); 
        
        return query.toString();
    }
    
     /**
     * Creates the map that holds two strings with a
     * field list names and field values list. The key for the first
     * is SQL_FIELDS_STRING, and for the other is
     * SQL_VALUES_STRING. Params is map with a bean properties
     * pairs (prop name/prop value).
     */
    @SuppressWarnings(value = "unchecked")
    public static Map                   sqlInsertStrings(Map<String, Object> props) {

        StringBuilder fieldList = new StringBuilder();
        StringBuilder valueList = new StringBuilder();
        
        for (Map.Entry<String, Object> entry : props.entrySet()) {
            String field = entry.getKey();
            // Convert any escape chars within the value string
            if (entry.getValue() != null) {
                String value = convertToSqlString(entry.getValue());

                if (fieldList.length() == 0) {
                    fieldList.append(field);
                    valueList.append(value);
                } else {
                    fieldList.append(", ").append(field);
                    valueList.append(", ").append(value);
                }
            }
        }
        HashMap insertParams = new HashMap();
        insertParams.put(DaoUtils.SQL_FIELDS_STRING, fieldList);
        insertParams.put(DaoUtils.SQL_VALUES_STRING, valueList);

        return insertParams;
    }
    
    /**
     * Converts the object to the string
     * with some (very minimal) formating.
     */
    public static String                convertToSqlString(Object value) {
        
        if (value == null) {
            return "";
        }
        String newValue;
        if (value instanceof Date) {
            SimpleDateFormat dateFormatter = new SimpleDateFormat("YYYY-mm-dd");
            newValue = dateFormatter.format((Date) value);
        } else if (value instanceof Boolean) {
            boolean bool = ((Boolean) value).booleanValue();
            if (bool) {
                newValue = "1";
            } else {
                newValue = "0";
            }
        } else {
            newValue = value.toString();
        }
        // remove all chars that can be part of numeric value, but not
        // a unicode numeric char (minus, decimal point, and comma)
        String tmp = StringUtils.removeStart(newValue, "-");
//        tmp = StringUtils.remove(tmp, '.');
        tmp = StringUtils.remove(tmp, ',');

        return StringUtils.isNumeric(tmp) ? (StringUtils.isBlank(newValue) ? "''" : newValue) : ("'" + StringEscapeUtils.escapeSql(newValue)) + "'";
    }

}
