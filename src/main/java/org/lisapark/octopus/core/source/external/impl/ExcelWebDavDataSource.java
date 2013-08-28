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
package org.lisapark.octopus.core.source.external.impl;

import static com.google.common.base.Preconditions.checkState;
import com.google.common.collect.Maps;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.json.JSONException;
import org.lisapark.octopus.core.Output;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.ProcessingException;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Attribute;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.event.EventType;
import org.lisapark.octopus.core.parameter.Constraints;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.ProcessingRuntime;
import org.lisapark.octopus.core.source.external.CompiledExternalSource;
import org.lisapark.octopus.core.source.external.ExternalSource;
import org.lisapark.octopus.util.json.ExcelUtils;
import org.lisapark.octopus.util.json.JsonUtils;
import org.openide.util.Exceptions;

/**
 * This class is an {@link ExternalSource} that is used to access relational databases. It can be configured with
 * a JDBC Url for the database, username, password, Driver fully qualified class name, and a query to execute.
 * <p/>
 * Currently, the source uses the {@link org.lisapark.octopus.core.Output#getEventType()} to get the names of the
 * columns and types of the columns, but it will probably be changed in the future to support a mapper that takes
 * a {@link ResultSet} and produces an {@link Event}.
 *
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public class ExcelWebDavDataSource extends ExternalSource {
    private static final String DEFAULT_NAME        = "Excel from Web DAV";
    private static final String DEFAULT_DESCRIPTION = "Excel from Web Dav Server as a source for events";

    private static final int URL_PARAMETER_ID = 1;
    private static final int USER_NAME_PARAMETER_ID = 2;
    private static final int PASSWORD_PARAMETER_ID = 3;
    private static final int DRIVER_PARAMETER_ID = 4;
    private static final int FILE_NAME_PARAMETER_ID = 5;

    private ExcelWebDavDataSource(UUID sourceId, String name, String description) {
        super(sourceId, name, description);
    }

    private ExcelWebDavDataSource(UUID sourceId, ExcelWebDavDataSource copyFromSource) {
        super(sourceId, copyFromSource);
    }

    private ExcelWebDavDataSource(ExcelWebDavDataSource copyFromSource) {
        super(copyFromSource);
    }

    @SuppressWarnings("unchecked")
    public void setUrl(String url) throws ValidationException {
        getParameter(URL_PARAMETER_ID).setValue(url);
    }

    public String getUrl() {
        return getParameter(URL_PARAMETER_ID).getValueAsString();
    }

    @SuppressWarnings("unchecked")
    public void setUsername(String username) throws ValidationException {
        getParameter(USER_NAME_PARAMETER_ID).setValue(username);
    }

    public String getUsername() {
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
    public void setDriverClass(String driverClass) throws ValidationException {
        getParameter(DRIVER_PARAMETER_ID).setValue(driverClass);
    }

    public String getDriverClass() {
        return getParameter(DRIVER_PARAMETER_ID).getValueAsString();
    }

    @SuppressWarnings("unchecked")
    public void setFileName(String query) throws ValidationException {
        getParameter(FILE_NAME_PARAMETER_ID).setValue(query);
    }

    public String getFileName() {
        return getParameter(FILE_NAME_PARAMETER_ID).getValueAsString();
    }

    public EventType getEventType() {
        return getOutput().getEventType();
    }

    @Override
    public ExcelWebDavDataSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new ExcelWebDavDataSource(sourceId, this);
    }

    @Override
    public ExcelWebDavDataSource copyOf() {
        return new ExcelWebDavDataSource(this);
    }

    public static ExcelWebDavDataSource newTemplate() {
        UUID sourceId = UUID.randomUUID();
        ExcelWebDavDataSource jdbc = new ExcelWebDavDataSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        jdbc.addParameter(Parameter.stringParameterWithIdAndName(URL_PARAMETER_ID, "URL").required(true));
        jdbc.addParameter(Parameter.stringParameterWithIdAndName(USER_NAME_PARAMETER_ID, "User name"));
        jdbc.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, "Password"));
        jdbc.addParameter(Parameter.stringParameterWithIdAndName(DRIVER_PARAMETER_ID, "Driver Class").required(true).
                constraint(Constraints.classConstraintWithMessage("%s is not a valid Driver Class")));
        jdbc.addParameter(Parameter.stringParameterWithIdAndName(FILE_NAME_PARAMETER_ID, "FileName").required(true));

        jdbc.setOutput(Output.outputWithId(1).setName("JSON_OUT"));

        return jdbc;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        validate();

        return new CompiledExcelWebDavDataSource(this.copyOf());
    }

    private static class CompiledExcelWebDavDataSource implements CompiledExternalSource {
        private final ExcelWebDavDataSource source;

        private volatile boolean running;

        public CompiledExcelWebDavDataSource(ExcelWebDavDataSource source) {
            this.source = source;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) throws ProcessingException {
            // this needs to be atomic, both the check and set
            synchronized (this) {
                checkState(!running, "Source is already processing events. Cannot call processEvents again");
                running = true;
            }

            List<String> jsonList = new ArrayList<String>();
            Properties props = new Properties();
            props.put(JsonUtils.URL, source.getUrl());
            props.put(JsonUtils.USER_NAME, source.getUsername());
            props.put(JsonUtils.PASSWORD, source.getPassword());
            props.put(JsonUtils.FILE_NAME, source.getFileName());
            props.put(JsonUtils.SQL, JsonUtils.FILE_NAME_QUERY);
            
            try {
                InputStream inputStream = ExcelUtils.inputStreamByNameFromWds(props);           
                
                JsonUtils jsonUtils = new JsonUtils() {

                    @Override
                    public List<String> dataFieldNames() {
                        throw new UnsupportedOperationException("Not supported yet.");
                    }

                    @Override
                    public List<String> treeNodeNames() {
                        throw new UnsupportedOperationException("Not supported yet.");
                    }
                };
                
                HSSFWorkbook workbook = ExcelUtils.excelWorkbookFromFile(inputStream);
                
                int i = 0;
                String json = jsonUtils.jsonStringFromSSbyIndex(workbook, i);                
                while(json != null){
                    jsonList.add(json);
                    i++;
                }
                if (jsonList.size() > 0) {
                    processResultSet(jsonList, runtime);
                }
            } catch (JSONException ex) {
                Exceptions.printStackTrace(ex);
            } catch (FileNotFoundException ex) {
                Exceptions.printStackTrace(ex);
            } catch (IOException ex) {
                Exceptions.printStackTrace(ex);
            } catch (SQLException ex) {
                Exceptions.printStackTrace(ex);
            }
        }

        void processResultSet(List<String> jsonList, ProcessingRuntime runtime) throws SQLException {
            Thread thread = Thread.currentThread();
            EventType eventType = source.getEventType();

            int i = 0;
            String json = jsonList.get(i);
            while (!thread.isInterrupted() && running && (json != null)) {
                
                Event newEvent = createEventFromJsonList(json, eventType);

                runtime.sendEventFromSource(newEvent, source);
            }
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }
        
        Event createEventFromJsonList(String json, EventType eventType) throws SQLException {
            Map<String, Object> attributeValues = Maps.newHashMap();

            for (Attribute attribute : eventType.getAttributes()) {
                Class type = attribute.getType();
                String attributeName = attribute.getName();

                if (type == String.class && "JSON".equalsIgnoreCase(attributeName)) {
                    String value = json;
                    attributeValues.put(attributeName, value);
                } else {
                    throw new IllegalArgumentException(String.format("Unknown attribute type %s", type));
                }
            }

            return new Event(attributeValues);
        }
    }
}