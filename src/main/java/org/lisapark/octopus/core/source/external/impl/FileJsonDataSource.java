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

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
import static com.google.common.base.Preconditions.checkState;
import com.google.common.collect.Maps;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
public class FileJsonDataSource extends ExternalSource {
    private static final String DEFAULT_NAME        = "Json from file";
    private static final String DEFAULT_DESCRIPTION = "Json File as a source for events";

    private static final int FILE_NAME_PARAMETER_ID     = 1;

    private FileJsonDataSource(UUID sourceId, String name, String description) {
        super(sourceId, name, description);
    }

    private FileJsonDataSource(UUID sourceId, FileJsonDataSource copyFromSource) {
        super(sourceId, copyFromSource);
    }

    private FileJsonDataSource(FileJsonDataSource copyFromSource) {
        super(copyFromSource);
    }

    @SuppressWarnings("unchecked")
    public void setFileName(String fileName) throws ValidationException {
        getParameter(FILE_NAME_PARAMETER_ID).setValue(fileName);
    }

    public String getFileName() {
        return getParameter(FILE_NAME_PARAMETER_ID).getValueAsString();
    }

    public EventType getEventType() {
        return getOutput().getEventType();
    }

    @Override
    public FileJsonDataSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new FileJsonDataSource(sourceId, this);
    }

    @Override
    public FileJsonDataSource copyOf() {
        return new FileJsonDataSource(this);
    }

    public static FileJsonDataSource newTemplate() {
        UUID sourceId = UUID.randomUUID();
        FileJsonDataSource jsonFileDatasource = new FileJsonDataSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        jsonFileDatasource.addParameter(Parameter.stringParameterWithIdAndName(FILE_NAME_PARAMETER_ID, "File Name").required(true));
        
        jsonFileDatasource.setOutput(Output.outputWithId(1).setName("JSON_OUT"));

        return jsonFileDatasource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        validate();

        return new CompiledExcelFileDataSource(this.copyOf());
    }

    private static class CompiledExcelFileDataSource implements CompiledExternalSource {
        private final FileJsonDataSource source;

        private volatile boolean running;

        public CompiledExcelFileDataSource(FileJsonDataSource source) {
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
            try {
                HSSFWorkbook workbook = ExcelUtils.excelWorkbookFromFile(source.getFileName());
                
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
            } catch (SQLException ex) {
                Exceptions.printStackTrace(ex);
            } catch (FileNotFoundException ex) {
                Exceptions.printStackTrace(ex);
            } catch (IOException ex) {
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