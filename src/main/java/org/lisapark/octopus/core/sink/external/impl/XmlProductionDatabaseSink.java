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
package org.lisapark.octopus.core.sink.external.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import javax.xml.parsers.ParserConfigurationException;
import org.lisapark.octopus.core.AbstractNode;
import org.lisapark.octopus.core.Input;
import org.lisapark.octopus.core.ProcessingException;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.SinkContext;
import org.lisapark.octopus.core.sink.external.CompiledExternalSink;
import org.lisapark.octopus.core.sink.external.ExternalSink;
import org.lisapark.octopus.core.source.Source;
import org.lisapark.octopus.util.gss.GssListUtils;
import org.lisapark.octopus.util.jdbc.Connections;
import org.lisapark.octopus.util.jdbc.DaoUtils;
import org.lisapark.octopus.util.xml.XmlConverterUtils;
import org.openide.util.Exceptions;
import org.w3c.dom.DOMException;
import org.xml.sax.SAXException;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class XmlProductionDatabaseSink extends AbstractNode implements ExternalSink {
    
    private final static java.util.logging.Logger logger 
            = java.util.logging.Logger.getLogger(XmlProductionDatabaseSink.class.getName());
    
    
     private static final String DEFAULT_NAME       = "XML производство в БД";
    private static final String DEFAULT_DESCRIPTION = "Вывод XML данных по производству в таблицу Базы Данных";
    
    private static final int URL_PARAMETER_ID       = 1;
    private static final int USER_NAME_PARAMETER_ID = 2;
    private static final int PASSWORD_PARAMETER_ID  = 3;
    private static final int DRIVER_PARAMETER_ID    = 4;
    private static final int TABLE_PARAMETER_ID     = 5;
    
    private static final int ATTRIBUTE_LIST_PARAMETER_ID = 6;
    private static final String ATTRIBUTE_LIST = "Перечень выходных атрибутов";
    private static final String ATTRIBUTE_LIST_DESCRIPTION = 
            "Список разделенных запятой имен атрибутов, которые необходимо вывести в Базу Данных. Этот параметр не может быть пустым.";

    private static final String URL = "URL";
    private static final String USER_NAME = "Имя Пользователя";
    private static final String PASSWORD = "Пароль";
    private static final String DRIVER = "Имя класса драйвера";
    private static final String TABLE = "Имя таблицы";
    
    private static final String DEFAULT_INPUT = "Входные данные";
    
    private static Map<Integer, String> paramMap = new HashMap<Integer, String>();

    private Input<Event> input;

    private XmlProductionDatabaseSink(UUID id, String name, String description) {
        super(id, name, description);
        input = Input.eventInputWithId(1);
        input.setName(DEFAULT_INPUT);
        input.setDescription(DEFAULT_INPUT);
    }

    private XmlProductionDatabaseSink(UUID id, XmlProductionDatabaseSink copyFromNode) {
        super(id, copyFromNode);
        input = copyFromNode.getInput().copyOf();
    }    
    
    @SuppressWarnings("unchecked")
    public void setUrl(String url) throws ValidationException {
        getParameter(URL_PARAMETER_ID).setValue(url);
    }

    public String getUrl() {
        return getParameter(URL_PARAMETER_ID).getValueAsString();
    }    
     
    @SuppressWarnings("unchecked")
    public void setUserName(String userName) throws ValidationException {
        getParameter(USER_NAME_PARAMETER_ID).setValue(userName);
    }

    public String getUserName() {
        return getParameter(USER_NAME_PARAMETER_ID).getValueAsString();
    }
     
    @SuppressWarnings("unchecked")
    public void setPassword(String password) throws ValidationException {
        getParameter(PASSWORD_PARAMETER_ID).setValue(password);
    }

    public String getPassword() {
        return getParameter(PASSWORD_PARAMETER_ID).getValueAsString();
    }
     
    @SuppressWarnings("unchecked")
    public void setDriver(String driver) throws ValidationException {
        getParameter(DRIVER_PARAMETER_ID).setValue(driver);
    }

    public String getDriver() {
        return getParameter(DRIVER_PARAMETER_ID).getValueAsString();
    }
   
    @SuppressWarnings("unchecked")
    public void setTable(String table) throws ValidationException {
        getParameter(TABLE_PARAMETER_ID).setValue(table);
    }

    private String getTable() {
        return getParameter(TABLE_PARAMETER_ID).getValueAsString();
    }

    @SuppressWarnings("unchecked")
    public void setAttributeList(String attributeList) throws ValidationException {
        getParameter(ATTRIBUTE_LIST_PARAMETER_ID).setValue(attributeList);
    }

    public String getAttributeList() {
        return getParameter(ATTRIBUTE_LIST_PARAMETER_ID).getValueAsString();
    }

    private XmlProductionDatabaseSink(XmlProductionDatabaseSink copyFromNode) {
        super(copyFromNode);
        this.input = copyFromNode.input.copyOf();
    }
   
    public Input<Event> getInput() {
        return input;
    }

    @Override
    public List<Input<Event>> getInputs() {
        return ImmutableList.of(input);
    }

    @Override
    public boolean isConnectedTo(Source source) {
        return input.isConnectedTo(source);
    }

    @Override
    public void disconnect(Source source) {
        if (input.isConnectedTo(source)) {
            input.clearSource();
        }
    }

    @Override
    public XmlProductionDatabaseSink newInstance() {
        return new XmlProductionDatabaseSink(UUID.randomUUID(), this);
    }

    @Override
    public XmlProductionDatabaseSink copyOf() {
        return new XmlProductionDatabaseSink(this);
    }

    public static XmlProductionDatabaseSink newTemplate() {
        UUID sinkId = UUID.randomUUID();

        XmlProductionDatabaseSink databaseSink = new XmlProductionDatabaseSink(sinkId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        
        databaseSink.addParameter(Parameter.stringParameterWithIdAndName(URL_PARAMETER_ID, URL).required(true)
                .defaultValue("jdbc:mysql://173.72.110.131:3306/IPLAST_PROD"));
        databaseSink.addParameter(Parameter.stringParameterWithIdAndName(USER_NAME_PARAMETER_ID, USER_NAME).required(true)
                .defaultValue("root"));
        databaseSink.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, PASSWORD).required(true)
                .defaultValue("lisa1234"));
        databaseSink.addParameter(Parameter.stringParameterWithIdAndName(DRIVER_PARAMETER_ID, DRIVER).required(true)
                .defaultValue("com.mysql.jdbc.Driver"));
        databaseSink.addParameter(Parameter.stringParameterWithIdAndName(TABLE_PARAMETER_ID, TABLE).required(true)
                .defaultValue("WORKTABLE"));
        databaseSink.addParameter(
                Parameter.stringParameterWithIdAndName(ATTRIBUTE_LIST_PARAMETER_ID, ATTRIBUTE_LIST)
                .defaultValue("RECORD_UUID,DATE,SHOP,SHIFT,MACHINE,PRODUCT,PRODUCT_TYPE,MATERIAL_TYPE,TOTAL_MATERIALS,TOTAL_PRODUCTS")
                .description(ATTRIBUTE_LIST_DESCRIPTION)
                );
        
        return databaseSink;
    }

    @Override
    public CompiledExternalSink compile() throws ValidationException {
        return new CompiledDatabaseSink(copyOf());
    }

    static class CompiledDatabaseSink extends CompiledExternalSink {
        
        private final XmlProductionDatabaseSink databaseSink;
        private Connection connection = null;
        
        GssListUtils gssList;
        
        protected CompiledDatabaseSink(XmlProductionDatabaseSink databaseSink) {
            super(databaseSink);            
            this.databaseSink = databaseSink;            
        }

        @Override
        public synchronized void processEvent(SinkContext ctx, Map<Integer, Event> eventsByInputId) {
            
            Event event = eventsByInputId.get(1);
                        
            if (event != null) {
                String attributeList = databaseSink.getAttributeList();
                
                logger.log(Level.INFO, "attributeList: ====> {0}", attributeList);
                
                List<Map<String, Object>> data = extractDataFromEvent(event, attributeList);
                
                logger.log(Level.INFO, "List<Map<String, Object>> data: ====> {0}", data);
                
//                Connection connection = null;
                try {
                    connection = getConnection(databaseSink.getDriver(), databaseSink.getUrl(),
                            databaseSink.getUserName(), databaseSink.getPassword());

                    String[] attList = attributeList.split(",");
                    String query = null;
                    for (Map item : data) {
                        HashMap<String, Object> dataMap = Maps.newHashMap();
                        for (int i = 0; i < attList.length; i++) {
                            String attr = attList[i];
                            if("RECORD_UUID".equalsIgnoreCase(attr)){
                                dataMap.put(attr, UUID.randomUUID().toString());                                
                            } else if("DATE".equalsIgnoreCase(attr)){
                                dataMap.put(attr, convertDate((String) item.get(attr)));
                            } else if(item.get(attr) != null){
                                dataMap.put(attr, (String) item.get(attr));
                            }                            
                        }                        
                        
                        query = DaoUtils.insertQueryString(dataMap, databaseSink.getTable(), null);
                        logger.log(Level.INFO, "Query: ====> {0}", query);
                        try {                        
                            int key = DaoUtils.insert(query, null, connection);
                        } catch (SQLException ex) {
                            logger.log(Level.INFO, "SQLException: ====> {0}", ex);
                            break;
                        }
                    }                    
                
                } catch (ProcessingException ex) {
                    Exceptions.printStackTrace(ex);
                }              
            } else {
                ctx.getStandardOut().println("event is null");
            }
        }

        private Connection getConnection(String className, String url, String userName, String password)  throws ProcessingException {
            
            try {
                Class.forName(className);
            } catch (ClassNotFoundException e) {
                // this should never happen since the parameter is constrained
                throw new ProcessingException("Could not find JDBC Driver Class " + className, e);
            }            

            try {
                if (connection == null) {
                    if (userName == null && password == null) {
                        connection = DriverManager.getConnection(url);
                    } else {
                        connection = DriverManager.getConnection(url, userName, password);
                    }
                }
            } catch (SQLException e) {
                throw new ProcessingException("Could not connect to database. Please check your settings.", e);
            }

            return connection;
        }

        private List<Map<String, Object>> extractDataFromEvent(Event event, String attributeList) {
            List<Map<String, Object>> list = null;
            try {
                Map<String, Object> eventMap = event.getData();                
                if(eventMap != null) {
                    list = XmlConverterUtils.eventsFromProductionXml((String)eventMap.get("XML"));
                } 
//                logger.log(Level.INFO, "XML: ====> {0}", eventMap.get("XML"));
            } catch (SAXException ex) {
                Exceptions.printStackTrace(ex);
            } catch (ParserConfigurationException ex) {
                Exceptions.printStackTrace(ex);
            } catch (IOException ex) {
                Exceptions.printStackTrace(ex);
            } catch (DOMException ex) {
                Exceptions.printStackTrace(ex);
            }
            return list;
        }
        
        @Override
        protected void finalize() throws Throwable{
            Connections.closeQuietly(connection);
            
            logger.log(Level.INFO, "Connection: ====> {0}", "Closed!!!");
            
            super.finalize();            
        }

        private String convertDate(String string) {
            String istring = string.replace('.', '-');
            String[] dateparts = istring.split("-");
            
//            logger.log(Level.INFO, "convertDate: ====> {0}", istring);
            
            if(dateparts.length == 3){
                return dateparts[2] + "-" + dateparts[1] + "-" + dateparts[0];
            } else {
                return "2012" + "-" + "06" + "-" + "13";
            }
        }
    }
}