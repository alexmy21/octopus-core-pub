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

import com.google.common.collect.Maps;
import com.google.gdata.util.ServiceException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.lisapark.octopus.core.Output;
import org.lisapark.octopus.core.ProcessingException;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Attribute;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.event.EventType;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.ProcessingRuntime;
import org.lisapark.octopus.core.source.external.CompiledExternalSource;
import org.lisapark.octopus.core.source.external.ExternalSource;
import org.lisapark.octopus.util.Booleans;
import org.lisapark.octopus.util.gss.GssListUtils;
import static com.google.common.base.Preconditions.checkState;
import java.util.logging.Level;
import org.openide.util.Exceptions;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class MongoDbSource  extends ExternalSource {
    private static final String DEFAULT_NAME        = "MongoDbSource";
    private static final String DEFAULT_DESCRIPTION = "Provides access to Mongo DB source for events";
    
    private static final int USER_NAME_PARAMETER_ID         = 1;
    private static final int PASSWORD_PARAMETER_ID          = 2;
    private static final int MONGO_HOST_PARAMETER_ID        = 3;
    private static final int MONGO_PORT_PARAMETER_ID        = 4;
    private static final int MONGO_DB_PARAMETER_ID          = 5;
    private static final int MONGO_COLLECTION_PARAMETER_ID  = 6;
    private static final int MONGO_CRITERIA_PARAMETER_ID    = 7;
    
     
    private final static java.util.logging.Logger logger 
            = java.util.logging.Logger.getLogger(GssListSource.class.getName());

    private MongoDbSource(UUID sourceId, String name, String description) {
        super(sourceId, name, description);
    }

    private MongoDbSource(UUID sourceId, MongoDbSource copyFromSource) {
        super(sourceId, copyFromSource);
    }

    private MongoDbSource(MongoDbSource copyFromSource) {
        super(copyFromSource);
    }

    @SuppressWarnings("unchecked")
    public void setWorkBook(String url) throws ValidationException {
        getParameter(MONGO_HOST_PARAMETER_ID).setValue(url);
    }

    public String getWorkBook() {
        return getParameter(MONGO_HOST_PARAMETER_ID).getValueAsString();
    }

    @SuppressWarnings("unchecked")
    public void setUseremail(String username) throws ValidationException {
        getParameter(USER_NAME_PARAMETER_ID).setValue(username);
    }

    public String getUseremail() {
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
    public void setSpreadSheet(String driverClass) throws ValidationException {
        getParameter(MONGO_PORT_PARAMETER_ID).setValue(driverClass);
    }

    public String getSpredSheet() {
        return getParameter(MONGO_PORT_PARAMETER_ID).getValueAsString();
    }

    @SuppressWarnings("unchecked")
    public void setSearchQueryString(String query) throws ValidationException {
        getParameter(MONGO_DB_PARAMETER_ID).setValue(query);
    }

    public String getSearchQueryString() {
        return getParameter(MONGO_DB_PARAMETER_ID).getValueAsString();
    }

    public EventType getEventType() {
        return getOutput().getEventType();
    }

    @Override
    public MongoDbSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new MongoDbSource(sourceId, this);
    }

    @Override
    public MongoDbSource copyOf() {
        return new MongoDbSource(this);
    }

    public static MongoDbSource newTemplate() {
        UUID sourceId = UUID.randomUUID();
        MongoDbSource mongoDbSource = new MongoDbSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
       
        mongoDbSource.addParameter(Parameter.stringParameterWithIdAndName(MONGO_HOST_PARAMETER_ID, "Mongo host").required(true));
        mongoDbSource.addParameter(Parameter.stringParameterWithIdAndName(MONGO_PORT_PARAMETER_ID, "Mongo port").required(true));
        mongoDbSource.addParameter(Parameter.stringParameterWithIdAndName(USER_NAME_PARAMETER_ID, "User name"));
        mongoDbSource.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, "Password"));
        mongoDbSource.addParameter(Parameter.stringParameterWithIdAndName(MONGO_DB_PARAMETER_ID, "Mongo Db").required(true));
        mongoDbSource.addParameter(Parameter.stringParameterWithIdAndName(MONGO_COLLECTION_PARAMETER_ID, "Mongo Collection").required(true));
        mongoDbSource.addParameter(Parameter.stringParameterWithIdAndName(MONGO_CRITERIA_PARAMETER_ID, "Mongo Criteria").required(true));

        mongoDbSource.setOutput(Output.outputWithId(1).setName("JSON_OUT"));

        return mongoDbSource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        validate();

        return new CompiledGssListSource(this.copyOf());
    }

    private static class CompiledGssListSource implements CompiledExternalSource {
        private final MongoDbSource source;

        private volatile boolean running;

        public CompiledGssListSource(MongoDbSource source) {
            this.source = source;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) throws ProcessingException {
            // this needs to be atomic, both the check and set
            synchronized (this) {                
                checkState(!running, "Source is already processing events. Cannot call processEvents again");
                running = true;
            }

            try {
                GssListUtils gssList = new GssListUtils(
                        source.getName(),
                        source.getWorkBook(),
                        source.getSpredSheet(),
                        source.getUseremail(),
                        source.getPassword());

                gssList.loadSheet();
                List<Map<String, Object>> data;

                if(source.getSearchQueryString()== null 
                        || source.getSearchQueryString().trim().isEmpty() 
                        || source.getSearchQueryString().contains("=")) {
                    data = gssList.endOfSheetQuery(source.getSearchQueryString());
                } else {
                    data = gssList.search(source.getSearchQueryString());                    
                }
                
logger.log(     Level.INFO, "gssCell.getCellRangeAsList;{0}", data.toString());

                processCellRange(data, runtime);
            } catch (IOException ex) {
                Exceptions.printStackTrace(ex);
            } catch (ServiceException ex) {
                Exceptions.printStackTrace(ex);
            }                   
        }

        void processCellRange(List<Map<String, Object>> cellRange, ProcessingRuntime runtime) {
            Thread thread = Thread.currentThread();
            EventType eventType = source.getEventType();

            int count = 0;
            int rangeRows = cellRange.size();
            
            while (!thread.isInterrupted() && running && count < rangeRows) {
                Event newEvent = createEventFromCellRange(cellRange.get(count), eventType, count);
                count++;

                runtime.sendEventFromSource(newEvent, source);
            }
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }

        Event createEventFromCellRange(Map<String, Object> cellRange, EventType eventType, int count) {
            Map<String, Object> attributeValues = Maps.newHashMap();

            for (Attribute attribute : eventType.getAttributes()) {
                Class type = attribute.getType();
                String attributeName = attribute.getName().trim();
                Object objValue = cellRange.get(attributeName.toLowerCase().replace("_", ""));

                if (type == String.class) {
                    String value = "";
                    if(objValue != null) {
                        value = (String) objValue;
                    }
                    attributeValues.put(attributeName, value);

                } else if (type == Integer.class) {
                    int value = 0;
                    if(objValue != null && !objValue.toString().trim().isEmpty()){
                        value = Integer.parseInt((String)objValue);
                    }
                    attributeValues.put(attributeName, value);

                } else if (type == Short.class) {
                    Short value = 0;
                    if(objValue != null && !objValue.toString().trim().isEmpty()){
                        value = Short.parseShort((String)objValue);
                    }
//                    short value = Short.parseShort((String)cellRange.get(count).get(attributeName));
                    attributeValues.put(attributeName, value);

                } else if (type == Long.class) {
                    Long value = 0L;
                    if(objValue != null && !objValue.toString().trim().isEmpty()){
                        value = Long.parseLong((String)objValue);
                    }
//                    long value = Long.parseLong((String)cellRange.get(count).get(attributeName));
                    attributeValues.put(attributeName, value);

                } else if (type == Double.class) {
                    Double value = 0D;
                    if(objValue != null && !objValue.toString().trim().isEmpty()){
                        value = Double.parseDouble((String)objValue);
                    }
//                    double value = Double.parseDouble((String)cellRange.get(count).get(attributeName));
                    attributeValues.put(attributeName, value);

                } else if (type == Float.class) {
                    Float value = 0F;
                    if(objValue != null && !objValue.toString().trim().isEmpty()){
                        value = Float.parseFloat((String)objValue);
                    }
//                    float value = Float.parseFloat((String)cellRange.get(count).get(attributeName));
                    attributeValues.put(attributeName, value);

                } else if (type == Boolean.class) {
                    String value = (String) cellRange.get(attributeName);
                    attributeValues.put(attributeName, Booleans.parseBoolean(value));
                } else {
                    throw new IllegalArgumentException(String.format("Unknown attribute type %s", type));
                }
            }

            return new Event(attributeValues);
        }

    }
}

