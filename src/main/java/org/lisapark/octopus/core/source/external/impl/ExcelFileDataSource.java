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
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import java.util.ArrayList;
import java.util.List;
import com.google.common.collect.Maps;
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

import org.lisapark.octopus.core.source.external.CompiledExternalSource;
import org.lisapark.octopus.core.source.external.ExternalSource;
import org.lisapark.octopus.util.json.JsonUtils;
import static com.google.common.base.Preconditions.checkState;
import org.lisapark.octopus.util.json.ExcelUtils;
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
public class ExcelFileDataSource extends ExternalSource {
    private static final String DEFAULT_NAME        = "Excel from file";
    private static final String DEFAULT_DESCRIPTION = "Excel as a source for events";

    private static final int FILE_NAME_PARAMETER_ID     = 1;
    private static final int SPREAD_SHEET_PARAMETER_ID  = 2;
    private static final int QUERY_PARAMETER_ID         = 3;

    private ExcelFileDataSource(UUID sourceId, String name, String description) {
        super(sourceId, name, description);
    }

    private ExcelFileDataSource(UUID sourceId, ExcelFileDataSource copyFromSource) {
        super(sourceId, copyFromSource);
    }

    private ExcelFileDataSource(ExcelFileDataSource copyFromSource) {
        super(copyFromSource);
    }

    @SuppressWarnings("unchecked")
    public void setFileName(String fileName) throws ValidationException {
        getParameter(FILE_NAME_PARAMETER_ID).setValue(fileName);
    }

    public String getFileName() {
        return getParameter(FILE_NAME_PARAMETER_ID).getValueAsString();
    }

    
    @SuppressWarnings("unchecked")
    public void setSpreadSheet(String spreadSheet) throws ValidationException {
        getParameter(SPREAD_SHEET_PARAMETER_ID).setValue(spreadSheet);
    }

    public String getSpreadSheet() {
        return getParameter(SPREAD_SHEET_PARAMETER_ID).getValueAsString();
    }


    @SuppressWarnings("unchecked")
    public void setQuery(String query) throws ValidationException {
        getParameter(QUERY_PARAMETER_ID).setValue(query);
    }

    public String getQuery() {
        return getParameter(QUERY_PARAMETER_ID).getValueAsString();
    }

    public EventType getEventType() {
        return getOutput().getEventType();
    }

    @Override
    public ExcelFileDataSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new ExcelFileDataSource(sourceId, this);
    }

    @Override
    public ExcelFileDataSource copyOf() {
        return new ExcelFileDataSource(this);
    }

    public static ExcelFileDataSource newTemplate() {
        UUID sourceId = UUID.randomUUID();
        ExcelFileDataSource jsonFileDatasource = new ExcelFileDataSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        jsonFileDatasource.addParameter(Parameter.stringParameterWithIdAndName(FILE_NAME_PARAMETER_ID, "File Name").required(true));
        jsonFileDatasource.addParameter(Parameter.stringParameterWithIdAndName(SPREAD_SHEET_PARAMETER_ID, "Spread Sheet").required(false));
        jsonFileDatasource.addParameter(Parameter.stringParameterWithIdAndName(QUERY_PARAMETER_ID, "Query").required(false));

        jsonFileDatasource.setOutput(Output.outputWithId(1).setName("JSON_OUT"));

        return jsonFileDatasource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        validate();

        return new CompiledExcelFileDataSource(this.copyOf());
    }

    private static class CompiledExcelFileDataSource implements CompiledExternalSource {
        private final ExcelFileDataSource source;

        private volatile boolean running;

        public CompiledExcelFileDataSource(ExcelFileDataSource source) {
            this.source = source;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) throws ProcessingException {
            // this needs to be atomic, both the check and set
            synchronized (this) {
                checkState(!running, "Source is already processing events. Cannot call processEvents again");
                running = true;
            }

            Thread thread = Thread.currentThread();
            EventType eventType = source.getOutput().getEventType();
            
            try {
                HSSFWorkbook workbook = ExcelUtils.excelWorkbookFromFile(source.getFileName());
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
                int i = 0;
                String json;
                
                while (true) {
                                      
                    if(!thread.isInterrupted() && running && workbook.getNumberOfSheets() > i){
                        json = jsonUtils.jsonStringFromSSbyIndex(workbook, i); 
                    } else {
                        break;
                    }                    
                    Event newEvent = createEventFromJsonList(json, eventType, i, workbook.getSheetName(i));
                    runtime.sendEventFromSource(newEvent, source);  
                    
                     i++; 
                }
                
            } catch (JSONException ex) {
                Exceptions.printStackTrace(ex);
            } catch (FileNotFoundException ex) {
                Exceptions.printStackTrace(ex);
            } catch (IOException ex) {
                Exceptions.printStackTrace(ex);
            } 
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }
        
        Event createEventFromJsonList(String json, EventType eventType, int index, String sheetName) {
            Map<String, Object> attributeValues = Maps.newHashMap();
            
            String attributeName;
            Class type;
            if (eventType.getAttributes().size() > index) {
                Attribute attribute = eventType.getAttributes().get(index);
                type = attribute.getType();
                attributeName = attribute.getName();
            } else {
                type = String.class;
                attributeName = sheetName;
            }

            if (type == String.class && attributeName.length() > 0) {
                String value = json;
                attributeValues.put(attributeName, value);
            } else {
                throw new IllegalArgumentException(String.format("Unknown attribute type %s", type));
            }

            return new Event(attributeValues);
        }
    }
}