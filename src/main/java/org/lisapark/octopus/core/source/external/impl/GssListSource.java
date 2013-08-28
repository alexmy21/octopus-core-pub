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
import com.google.gdata.util.ServiceException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
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
import org.openide.util.Exceptions;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class GssListSource  extends ExternalSource {
    private static final String DEFAULT_NAME            = "Google Spreadsheet";
    private static final String DEFAULT_DESCRIPTION     = "Provides access to Google Spreadsheet.";
    
    private static final String DEFAULT_ROW_NUM_NAME    = "ROWNUM";
    
    private static final int USER_EMAIL_PARAMETER_ID            = 1;
    private static final int PASSWORD_PARAMETER_ID              = 2;
    private static final int WORK_BOOK_PARAMETER_ID             = 3;
    private static final int SPREAD_SHEET_PARAMETER_ID          = 4;
    private static final int END_OF_SHEET_QUERY_PARAMETER_ID    = 5;
//    private static final int ROW_NAME_PARAMETER_ID          = 6;
    
    private static final int ROW_LAST_PARAMETER_ID          = 7;
    private static final int ROW_START_PARAMETER_ID         = 8;
    private static final int ROW_END_PARAMETER_ID           = 9;
    
     
    private final static java.util.logging.Logger logger 
            = java.util.logging.Logger.getLogger(GssListSource.class.getName());

    private GssListSource(UUID sourceId, String name, String description) {
        super(sourceId, name, description);
    }

    private GssListSource(UUID sourceId, GssListSource copyFromSource) {
        super(sourceId, copyFromSource);
    }

    private GssListSource(GssListSource copyFromSource) {
        super(copyFromSource);
    }

    @SuppressWarnings("unchecked")
    public void setWorkBook(String url) throws ValidationException {
        getParameter(WORK_BOOK_PARAMETER_ID).setValue(url);
    }

    public String getWorkBook() {
        return getParameter(WORK_BOOK_PARAMETER_ID).getValueAsString();
    }

    @SuppressWarnings("unchecked")
    public void setUseremail(String username) throws ValidationException {
        getParameter(USER_EMAIL_PARAMETER_ID).setValue(username);
    }

    public String getUseremail() {
        return getParameter(USER_EMAIL_PARAMETER_ID).getValueAsString();
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
        getParameter(SPREAD_SHEET_PARAMETER_ID).setValue(driverClass);
    }

    public String getSpredSheet() {
        return getParameter(SPREAD_SHEET_PARAMETER_ID).getValueAsString();
    }

    @SuppressWarnings("unchecked")
    public void setEndOfSheetQuery(String query) throws ValidationException {
        getParameter(END_OF_SHEET_QUERY_PARAMETER_ID).setValue(query);
    }

    public String getEndOfSheetQuery() {
        return getParameter(END_OF_SHEET_QUERY_PARAMETER_ID).getValueAsString();
    }
    
    
//    @SuppressWarnings("unchecked")
//    public void setRow(String rowName) throws ValidationException {
//        getParameter(ROW_NAME_PARAMETER_ID).setValue(rowName);
//    }
//
//    public String getRowName() {
//        String rowName = getParameter(ROW_NAME_PARAMETER_ID).getValueAsString();
//        if(rowName == null || rowName.isEmpty()) rowName = DEFAULT_ROW_NUM_NAME;
//        return rowName;
//    }

    @SuppressWarnings("unchecked")
    public void setRowLast(Integer rowLast) throws ValidationException {
        getParameter(ROW_LAST_PARAMETER_ID).setValue(rowLast);
    }

    public Integer getRowLast() {
        return getParameter(ROW_LAST_PARAMETER_ID).getValueAsInteger();
    }

    @SuppressWarnings("unchecked")
    public void setRowStart(Integer rowLast) throws ValidationException {
        getParameter(ROW_START_PARAMETER_ID).setValue(rowLast);
    }

    public Integer getRowStart() {
        return getParameter(ROW_START_PARAMETER_ID).getValueAsInteger();
    }

    @SuppressWarnings("unchecked")
    public void setRowEnd(Integer rowLast) throws ValidationException {
        getParameter(ROW_END_PARAMETER_ID).setValue(rowLast);
    }

    public Integer getRowEnd() {
        return getParameter(ROW_END_PARAMETER_ID).getValueAsInteger();
    }

    public EventType getEventType() {
        return getOutput().getEventType();
    }

    @Override
    public GssListSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new GssListSource(sourceId, this);
    }

    @Override
    public GssListSource copyOf() {
        return new GssListSource(this);
    }

    public static GssListSource newTemplate() {
        UUID sourceId = UUID.randomUUID();
        GssListSource gssSource = new GssListSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
       
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(USER_EMAIL_PARAMETER_ID, "User email: "));
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, "Password: "));
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(WORK_BOOK_PARAMETER_ID, "Spreadsheet name: ")
                .required(true));
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(SPREAD_SHEET_PARAMETER_ID, "Sheet name: ")
                .required(true));
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(END_OF_SHEET_QUERY_PARAMETER_ID, "End of sheet criteria: ")
                .description("Field name that defines sheet end.")
                .required(false));
//        gssSource.addParameter(Parameter.stringParameterWithIdAndName(ROW_NAME_PARAMETER_ID, "Имя поля для номера строки: ")
//                .required(true));
        
        gssSource.addParameter(Parameter.integerParameterWithIdAndName(ROW_LAST_PARAMETER_ID, "Number of rows from the end: ")
//                .description("Количество строк от конца таблицы, которые должны быть загружены в Модель. Если значение этого свойства"
//                + " больше нуля, свойства - начальная строка и последняя строка - игнорируются. В качестве последней строки принимается"
//                + " последняя строка таблицы."
//                )
                .defaultValue(0)
                .required(false));
        gssSource.addParameter(Parameter.integerParameterWithIdAndName(ROW_START_PARAMETER_ID, "Start row: ")
//                .description("Указывает начальную строку, с которой начнется загрузка в модель. Эта строка является первой в загружаемой последовательности.")
                .defaultValue(0)
                .required(false));
        gssSource.addParameter(Parameter.integerParameterWithIdAndName(ROW_END_PARAMETER_ID, "End row: ")
//                .description("Указывает последнюю строку, которой оканчивается загрузка в модель. Эта строка является последней в загружаемой последовательности.")
                .defaultValue(0)
                .required(false));

        gssSource.setOutput(Output.outputWithId(1).setName("Output data"));

        return gssSource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        validate();

        return new CompiledGssListSource(this.copyOf());
    }

    private static class CompiledGssListSource implements CompiledExternalSource {
        private final GssListSource source;

        private volatile boolean running;

        public CompiledGssListSource(GssListSource source) {
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
                GssListUtils gssList = new GssListUtils(source.getName(), source.getWorkBook(),
                        source.getSpredSheet(), source.getUseremail(), source.getPassword());

                gssList.loadSheet();
                List<Map<String, Object>> data;

                data = gssList.endOfSheetQuery(source.getEndOfSheetQuery());

                int start   = 0;
                int end     = data.size() - 1;
                
                // Define row range
                if (source.getRowLast() != null && source.getRowLast() > 0
                        && source.getRowLast() < end) {
                    start = end - source.getRowLast();
                } else {
                    if (source.getRowStart() != null 
                            && source.getRowStart() > 0
                            && source.getRowStart() < end) {
                        start = source.getRowStart();
                    }

                    if (source.getRowEnd() != null && source.getRowEnd() > 0
                            && source.getRowEnd() <= end) {
                        end = source.getRowEnd();
                    }
                }   
                
logger.log(     Level.INFO, "gssCell.getCellRangeAsList;{0}", data.toString());

                processCellRange(data, runtime, start, end);
            } catch (IOException ex) {
                Exceptions.printStackTrace(ex);
            } catch (ServiceException ex) {
                Exceptions.printStackTrace(ex);
            }                   
        }

        void processCellRange(List<Map<String, Object>> data, ProcessingRuntime runtime, 
                int start, int end) {
            Thread thread = Thread.currentThread();
            EventType eventType = source.getEventType();

            for(int i = start; i <= end; i++){
                 
                if(thread.isInterrupted()) {
                    break;
                }
                
                Event newEvent = createEventFromCellRange(data.get(i), eventType);
  logger.log(     Level.INFO, "processCellRange: ==> {0}", newEvent);              
                runtime.sendEventFromSource(newEvent, source);
            }
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }

        Event createEventFromCellRange(Map<String, Object> cellRange, EventType eventType) {
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
            attributeValues.put(GssListUtils.REC_NUMBER_NAME, 
                    new Integer(cellRange.get(GssListUtils.REC_NUMBER_NAME).toString()));

            return new Event(attributeValues);
        }
        
        private int convert2Int(Object obj) throws NumberFormatException {
            int recNum;
            if(obj instanceof Integer){
                recNum = (Integer) obj;
            } else if(obj instanceof Double){
                recNum = (int) Math.floor((Double)obj);
            } else if(obj instanceof Float){
                recNum = (int) Math.floor((Float)obj);
            } else {
                recNum = Integer.parseInt(obj.toString());
            }
            return recNum;
        }
    }    
}
