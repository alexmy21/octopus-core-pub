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

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
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
import org.openide.util.Exceptions;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class JsonDatabaseSink extends AbstractNode implements ExternalSink {
    
    private final static java.util.logging.Logger logger 
            = java.util.logging.Logger.getLogger(DatabaseSink.class.getName());
    
    private static final String DEFAULT_NAME        = "JsonDatabaseSink";
    private static final String DEFAULT_DESCRIPTION = "Output Json Jbject as a String to Database";
    
    private static final int URL_PARAMETER_ID       = 1;
    private static final int USER_NAME_PARAMETER_ID = 2;
    private static final int PASSWORD_PARAMETER_ID  = 3;
    private static final int DRIVER_PARAMETER_ID    = 4;
    private static final int TABLE_PARAMETER_ID     = 5;
    
    private static final int ATTRIBUTE_LIST_PARAMETER_ID = 6;
    private static final String ATTRIBUTE_LIST = "Show Attributes";
    private static final String ATTRIBUTE_LIST_DESCRIPTION = 
            "List comma separated attribute names that you would like to show on Console. Empty - will show all attributes.";

    private static final String URL = "URL";
    private static final String USER_NAME = "User name";
    private static final String PASSWORD = "Password";
    private static final String DRIVER = "Driver class name";
    private static final String TABLE = "Table name";
    
    private static final String DEFAULT_INPUT = "JSON_IN";
    
    private static Map<Integer, String> paramMap = new HashMap<Integer, String>();

    private Input<Event> input;

    private JsonDatabaseSink(UUID id, String name, String description) {
        super(id, name, description);
        input = Input.eventInputWithId(1);
        input.setName(DEFAULT_INPUT);
        input.setDescription(DEFAULT_INPUT);
    }

    private JsonDatabaseSink(UUID id, JsonDatabaseSink copyFromNode) {
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

    private JsonDatabaseSink(JsonDatabaseSink copyFromNode) {
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
    public JsonDatabaseSink newInstance() {
        return new JsonDatabaseSink(UUID.randomUUID(), this);
    }

    @Override
    public JsonDatabaseSink copyOf() {
        return new JsonDatabaseSink(this);
    }

    public static JsonDatabaseSink newTemplate() {
        UUID sinkId = UUID.randomUUID();

        JsonDatabaseSink databaseSink = new JsonDatabaseSink(sinkId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        
        databaseSink.addParameter(Parameter.stringParameterWithIdAndName(URL_PARAMETER_ID, URL).required(true));
        databaseSink.addParameter(Parameter.stringParameterWithIdAndName(USER_NAME_PARAMETER_ID, USER_NAME).required(true));
        databaseSink.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, PASSWORD).required(true));
        databaseSink.addParameter(Parameter.stringParameterWithIdAndName(DRIVER_PARAMETER_ID, DRIVER).required(true));
        databaseSink.addParameter(Parameter.stringParameterWithIdAndName(TABLE_PARAMETER_ID, TABLE).required(true));
        databaseSink.addParameter(
                Parameter.stringParameterWithIdAndName(ATTRIBUTE_LIST_PARAMETER_ID, ATTRIBUTE_LIST)
                .description(ATTRIBUTE_LIST_DESCRIPTION)
                );
        
        return databaseSink;
    }

    @Override
    public CompiledExternalSink compile() throws ValidationException {
        return new CompiledDatabaseSink(copyOf());
    }

    static class CompiledDatabaseSink extends CompiledExternalSink {
        
        private final JsonDatabaseSink databaseSink;
        private Connection connection = null;
        
        GssListUtils gssList;
        
        protected CompiledDatabaseSink(JsonDatabaseSink databaseSink) {
            super(databaseSink);            
            this.databaseSink = databaseSink;            
        }

        @Override
        public synchronized void processEvent(SinkContext ctx, Map<Integer, Event> eventsByInputId) {
            
            Event event = eventsByInputId.get(1);
                        
            if (event != null) {
                String attributeList = databaseSink.getAttributeList();
                Map<String, Object> data = extractDataFromEvent(event, attributeList);
                
//                Connection connection = null;
                try {
                    connection = getConnection(databaseSink.getDriver(), databaseSink.getUrl(),
                            databaseSink.getUserName(), databaseSink.getPassword());

                    String query = DaoUtils.insertQueryString(data, databaseSink.getTable(), null);
                    
                    logger.log(Level.INFO, "Query: ====> {0}", query);
                    
                    int key = DaoUtils.insert(query, null, connection);              
                   
                } catch (SQLException ex) {
                    Exceptions.printStackTrace(ex);
                } catch (ProcessingException ex) {
                    Exceptions.printStackTrace(ex);
                } 
//                finally {
//                     Connections.closeQuietly(connection);
//                }               
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

        private Map<String, Object> extractDataFromEvent(Event event, String attributeList) {
            Map<String, Object> retMap = Maps.newHashMap();
            Map<String, Object> eventMap = event.getData();
            
            String[] attList = attributeList.split(",");
            
            for(int i = 0; i < attList.length; i++){
                String attr = attList[i].trim();                
                retMap.put(attr, eventMap.get(attr));
            }            
            
            return retMap;
        }
        
        @Override
        protected void finalize() throws Throwable{
            Connections.closeQuietly(connection);
            
            logger.log(Level.INFO, "Connection: ====> {0}", "Closed!!!");
            
            super.finalize();            
        }
    }
}
