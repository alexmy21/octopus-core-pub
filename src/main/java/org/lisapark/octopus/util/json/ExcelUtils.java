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
package org.lisapark.octopus.util.json;

import com.google.common.collect.Lists;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.lisapark.octopus.core.ProcessingException;
import org.openide.util.Exceptions;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class ExcelUtils {
    
    public static final String CLASS_NAME           = "classname";
    public static final String URL                  = "url";
    public static final String USER_NAME            = "username";
    public static final String PASSWORD             = "password";
    public static final String SELECT_SQL           = "selectsql";
    public static final String UPDATE_SQL           = "updatesql";
    public static final String SHEET_INDEX          = "sheetindex";
    public static final String FILE_NAME            = "filename";
    public static final String OUTLINE_INCREAMENT   = "increament";
    
    /**
     * 
     * @param fileName
     * @return
     * @throws FileNotFoundException
     * @throws IOException 
     */
    public static HSSFWorkbook excelWorkbookFromFile(String fileName) 
            throws FileNotFoundException, IOException{
        return  new HSSFWorkbook(new FileInputStream(fileName));
    }
    
    /**
     * 
     * @param fileInputStream
     * @return
     * @throws FileNotFoundException
     * @throws IOException 
     */
    public static HSSFWorkbook excelWorkbookFromFile(InputStream fileInputStream) 
            throws FileNotFoundException, IOException{
        return  new HSSFWorkbook(fileInputStream);
    }
    
    /**
     * 
     * @param fileInputStream
     * @return
     * @throws FileNotFoundException
     * @throws IOException 
     */
    public static HSSFWorkbook getWorkbookFromFile(FileInputStream fileInputStream) 
            throws FileNotFoundException, IOException{
        return  new HSSFWorkbook(fileInputStream);
    }
   
    /**
     * Returns UUID using provided query
     * 
     * @param properties
     * @param query
     * @return 
     */
    public static String fileIdFromWebDavServer(Properties properties) 
            throws ProcessingException, SQLException{
        
        String fileUUID = null;
        
        String className    = properties.getProperty(CLASS_NAME);
        String url          = properties.getProperty(URL);
        String userName     = properties.getProperty(USER_NAME);
        String password     = properties.getProperty(PASSWORD);
        String selectSql    = properties.getProperty(SELECT_SQL);
        String fileName     = properties.getProperty(FILE_NAME);
        
        Connection conn = getConnection(className, url, userName, password);
        
        PreparedStatement stmt = conn.prepareStatement(selectSql);

        stmt.setNString(1, fileName);

        ResultSet resultSet = stmt.executeQuery();

        if (resultSet.next()) {
            fileUUID = resultSet.getString(1);
        }
        
        return fileUUID;
    }
     
    /**
     * Returns file from Web Dav Server as InputStream using provided 
     * properties. There are two ways to identify selected file:
     * - select query;
     * - UUID of the file in Web Dav Server database;
     * 
     * They use two different properties: "query" and "UUID".
     * Only one of this properties can be not empty at any time.
     * 
     * The other properties are standard properties for Connection string:
     * - url;
     * - username;
     * - password
     * 
     * @param properties
     * @return 
     */
    public static synchronized InputStream inputStreamByNameFromWds(Properties properties) 
            throws ProcessingException, SQLException{
        InputStream inputStream = null;
        
        String className    = properties.getProperty(CLASS_NAME);
        String url          = properties.getProperty(URL);
        String userName     = properties.getProperty(USER_NAME);
        String password     = properties.getProperty(PASSWORD);
        String selectSql    = properties.getProperty(SELECT_SQL);
        String fileName     = properties.getProperty(FILE_NAME);
        
        Connection conn = getConnection(className, url, userName, password);
        
        PreparedStatement stmt = conn.prepareStatement(selectSql);

        stmt.setNString(1, fileName);

        ResultSet resultSet = stmt.executeQuery();

        if (resultSet.next()) {
            inputStream = resultSet.getBinaryStream(1);
        }
        
        return inputStream;
    }
    
    /**
     * 
     * @param className
     * @param url
     * @param userName
     * @param password
     * @return
     * @throws ProcessingException
     * @throws SQLException 
     */
    private static Connection getConnection(String className, String url, String userName, String password) 
            throws ProcessingException, SQLException {

        try {
            Class.forName(className);
        } catch (ClassNotFoundException e) {
            // this should never happen since the parameter is constrained
            throw new ProcessingException("Could not find JDBC Driver Class " + className, e);
        }

        Connection connection;

        try {
            if (userName == null && password == null) {
                connection = DriverManager.getConnection(url);
            } else {
                connection = DriverManager.getConnection(url, userName, password);
            }
        } catch (SQLException e) {
            throw new ProcessingException("Could not connect to database. Please check your settings.", e);
        }

        return connection;
    }

    public static List<HSSFWorkbook> allUnscannedExcelWorkbooks(Properties properties) {

        List<HSSFWorkbook> list = Lists.newArrayList();
        List<String> listId = Lists.newArrayList();

        String className    = properties.getProperty(CLASS_NAME);
        String url          = properties.getProperty(URL);
        String userName     = properties.getProperty(USER_NAME);
        String password     = properties.getProperty(PASSWORD);
        String selectSql    = properties.getProperty(SELECT_SQL);
        String updateSql    = properties.getProperty(UPDATE_SQL);
        String fileName     = properties.getProperty(FILE_NAME);
        
        Connection conn     = null;
        try {
            conn = getConnection(className, url, userName, password);
            PreparedStatement selectStmt = conn.prepareStatement(selectSql);
            PreparedStatement updateStmt = conn.prepareStatement(updateSql);
            
            selectStmt.setNString(1, fileName + "%");

            ResultSet resultSet = selectStmt.executeQuery();

            // Select all Excel docs as HSSF work book onbjects
            while (resultSet.next()) {
                HSSFWorkbook book = new HSSFWorkbook(resultSet.getBinaryStream(1));
                list.add(book);
                String id = resultSet.getString(2);
                listId.add(id);
            }
            // Mark all read Excel docs as SCANNED
            for(String item : listId){
                updateStmt.setNString(1, item);
                updateStmt.execute();
            }
        } catch (IOException ex) {
            Exceptions.printStackTrace(ex);
        } catch (ProcessingException ex) {
            Exceptions.printStackTrace(ex);
        } catch (SQLException ex) {
            Exceptions.printStackTrace(ex);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    // Do nothing
                }
            }
        }
        return list;
    }
}
