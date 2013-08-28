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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Level;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.json.JSONException;
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
import org.lisapark.octopus.util.json.ExcelUtils;
import org.lisapark.octopus.util.xml.XmlConverterUtils;
import org.openide.util.Exceptions;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class ExcelWarehouseSource_old  extends ExternalSource {
    private static final String DEFAULT_NAME            = "Excel документ по складам";
    private static final String DEFAULT_DESCRIPTION     = 
//            "Provides access to Excel Warehouse data Spread Sheet source for events. Excel document is located in Web DAV Server."
            "Обеспечивает доступ к Электронным таблицам с данными по складам. Документы находятся на Web DAV Server.";
    
    // Web DAV Server parameters
    private static final int CLASS_NAME_PARAMETER_ID    = 1;
    private static final int URL_PARAMETER_ID           = 2;
    private static final int USER_NAME_PARAMETER_ID     = 3;
    private static final int PASSWORD_PARAMETER_ID      = 4;
    private static final int INCREAMENT_PARAMETER_ID    = 5;
    private static final int FILE_NAME_PARAMETER_ID     = 6;
    private static final int SHEET_INDEX_PARAMETER_ID   = 7;
    
    // Default queries to Web DAV Server Database
    public static final String FILE_SELECT_QUERY        = "SELECT CONTENT, ID FROM REPOSITORY WHERE NAME LIKE ?";
    public static final String FILE_UPDATE_QUERY        = "UPDATE REPOSITORY SET SCANNED = 1 WHERE ID = ?";
    
    
     
    private final static java.util.logging.Logger logger 
            = java.util.logging.Logger.getLogger(ExcelWarehouseSource_old.class.getName());

    private ExcelWarehouseSource_old(UUID sourceId, String name, String description) {
        super(sourceId, name, description);
    }

    private ExcelWarehouseSource_old(UUID sourceId, ExcelWarehouseSource_old copyFromSource) {
        super(sourceId, copyFromSource);
    }

    private ExcelWarehouseSource_old(ExcelWarehouseSource_old copyFromSource) {
        super(copyFromSource);
    }

    @SuppressWarnings("unchecked")
    public void setClassName(String className) throws ValidationException {
        getParameter(CLASS_NAME_PARAMETER_ID).setValue(className);
    }

    public String getClassName() {
        return getParameter(CLASS_NAME_PARAMETER_ID).getValueAsString();
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
    public void setIncreament(Integer increament) throws ValidationException {
        getParameter(INCREAMENT_PARAMETER_ID).setValue(increament);
    }

    public Integer getIncreament() {
        return getParameter(INCREAMENT_PARAMETER_ID).getValueAsInteger();
    }
    
    @SuppressWarnings("unchecked")
    public void setFileName(String fileName) throws ValidationException {
        getParameter(FILE_NAME_PARAMETER_ID).setValue(fileName);
    }

    public String getFileName() {
        return getParameter(FILE_NAME_PARAMETER_ID).getValueAsString();
    }
    
    @SuppressWarnings("unchecked")
    public void setSheetIndex(Integer index) throws ValidationException {
        getParameter(SHEET_INDEX_PARAMETER_ID).setValue(index);
    }

    public Integer getSheetIndex() {
        return getParameter(SHEET_INDEX_PARAMETER_ID).getValueAsInteger();
    }

    
    public EventType getEventType() {
        return getOutput().getEventType();
    }

    @Override
    public ExcelWarehouseSource_old newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new ExcelWarehouseSource_old(sourceId, this);
    }

    @Override
    public ExcelWarehouseSource_old copyOf() {
        return new ExcelWarehouseSource_old(this);
    }

    public static ExcelWarehouseSource_old newTemplate() {
        UUID sourceId = UUID.randomUUID();
        ExcelWarehouseSource_old gssSource = new ExcelWarehouseSource_old(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
       
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(CLASS_NAME_PARAMETER_ID, "Имя класса для Драйвера")
                .defaultValue("com.mysql.jdbc.Driver").required(true));
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(URL_PARAMETER_ID, "URL").required(true)
                .defaultValue("jdbc:mysql://173.72.110.131:3306/webdav"));
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(USER_NAME_PARAMETER_ID, "Имя пользователя").required(true)
                .defaultValue("root"));
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, "Password").required(true)
                .defaultValue("lisa1234"));
//        gssSource.addParameter(Parameter.stringParameterWithIdAndName(SQL_PARAMETER_ID, "Search/Query String").required(true)
//                .defaultValue(""));
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(FILE_NAME_PARAMETER_ID, "Имя Excel файла").required(true)
                .description("Укажите только часть имени файла без даты и расширения.")
                .defaultValue("warehouse"));
        gssSource.addParameter(Parameter.integerParameterWithIdAndName(SHEET_INDEX_PARAMETER_ID, "Индекс таблицы").required(true)
                .description("Индекс электронной таблицы в данной рабочей книге.")
                .defaultValue(0));
        gssSource.addParameter(Parameter.integerParameterWithIdAndName(INCREAMENT_PARAMETER_ID, "Отступ для групп").required(true)
                .description("Шаг отступа, который отражает группировки для элементов из первой колонки.")
                .defaultValue(1));

        gssSource.setOutput(Output.outputWithId(1).setName("XML"));

        return gssSource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        validate();

        return new CompiledGssListSource(this.copyOf());
    }

    private static class CompiledGssListSource implements CompiledExternalSource {
        private final ExcelWarehouseSource_old source;
        private volatile boolean running;

        public CompiledGssListSource(ExcelWarehouseSource_old source) {
            this.source = source;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) throws ProcessingException {
            // this needs to be atomic, both the check and set
            synchronized (this) {
                checkState(!running, "Source is already processing events. Cannot call processEvents again");
                running = true;
            }
            
            Properties props = new Properties();

            props.put(ExcelUtils.CLASS_NAME, source.getClassName());
            props.put(ExcelUtils.USER_NAME, source.getUserName());
            props.put(ExcelUtils.PASSWORD, source.getPassword());
            props.put(ExcelUtils.URL, source.getUrl());
            props.put(ExcelUtils.SELECT_SQL, ExcelWarehouseSource_old.FILE_SELECT_QUERY);
            props.put(ExcelUtils.UPDATE_SQL, ExcelWarehouseSource_old.FILE_UPDATE_QUERY);
            props.put(ExcelUtils.FILE_NAME, source.getFileName());
//                props.put(ExcelUtils.SHEET_INDEX, source.getSheetIndex());
//                props.put(ExcelUtils.OUTLINE_INCREAMENT, source.getIncreament());

            List<HSSFWorkbook> data = ExcelUtils.allUnscannedExcelWorkbooks(props);

            logger.log(Level.INFO, "data size is {0}", data.size());

            processCellRange(data, runtime);
        }

        void processCellRange(List<HSSFWorkbook> workbookList, ProcessingRuntime runtime) {
            Thread thread = Thread.currentThread();
            EventType eventType = source.getEventType();

            int count = 0;
            int index = source.getSheetIndex();
            int increament = source.getIncreament();
            
            int workbookCount = workbookList.size();
            
            while (!thread.isInterrupted() && running && count < workbookCount) {
                Event newEvent = createEventFromSpreadsheet(workbookList.get(count), eventType, index, increament);
                count++;
                runtime.sendEventFromSource(newEvent, source);
            }
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }

        Event createEventFromSpreadsheet(HSSFWorkbook workbook, EventType eventType, int index, int increament) {
            Map<String, Object> attributeValues = Maps.newHashMap();

            for (Attribute attribute : eventType.getAttributes()) {
                try {
                    Class type = attribute.getType();
                    String attributeName = attribute.getName().trim();
                    String xmlString = XmlConverterUtils.xmlStringFromWarehouseSS(workbook, index, increament);

                    if (type == String.class) {
                        String value = "";
                        if(xmlString != null) {
                            value = xmlString;
                        }
                        attributeValues.put(attributeName, value);
                    }  else {
                        throw new IllegalArgumentException(String.format("Unknown attribute type %s", type));
                    }
                } catch (JSONException ex) {
                    Exceptions.printStackTrace(ex);
                } catch (FileNotFoundException ex) {
                    Exceptions.printStackTrace(ex);
                } catch (IOException ex) {
                    Exceptions.printStackTrace(ex);
                } catch(IllegalArgumentException ex){
                    Exceptions.printStackTrace(ex);
                }
            }

            return new Event(attributeValues);
        }

    }
}
