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
import com.googlecode.sardine.DavResource;
import com.googlecode.sardine.Sardine;
import com.googlecode.sardine.SardineFactory;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.lisapark.octopus.core.Output;
import org.lisapark.octopus.core.ProcessingException;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.event.EventType;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.ProcessingRuntime;
import org.lisapark.octopus.core.source.external.CompiledExternalSource;
import org.lisapark.octopus.core.source.external.ExternalSource;
import org.lisapark.octopus.util.json.ExcelSardineUtils;
import org.openide.util.Exceptions;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class ExcelWarehouseSource  extends ExternalSource {
    private static final String DEFAULT_NAME            = "Excel документ о складах";
    private static final String DEFAULT_DESCRIPTION     = 
            "Обеспечивает доступ к Электронным таблицам с данными о складах. Документы находятся на Web DAV Server."
            ;
    
    // Web DAV Server parameters
    private static final int USER_NAME_PARAMETER_ID     = 3;
    private static final int PASSWORD_PARAMETER_ID      = 4;
    private static final int INCREAMENT_PARAMETER_ID    = 5;
    private static final int FILE_NAME_PARAMETER_ID     = 6;
    private static final int SHEET_INDEX_PARAMETER_ID   = 7;
    
    private static final int DATE_ROW_PARAMETER_ID      = 8;
    private static final int START_COL_PARAMETER_ID     = 9;
    private static final int DATE_SHIFT_PARAMETER_ID    = 10;
    
    private final static java.util.logging.Logger logger 
            = java.util.logging.Logger.getLogger(ExcelWarehouseSource.class.getName());

    private ExcelWarehouseSource(UUID sourceId, String name, String description) {
        super(sourceId, name, description);
    }

    private ExcelWarehouseSource(UUID sourceId, ExcelWarehouseSource copyFromSource) {
        super(sourceId, copyFromSource);
    }

    private ExcelWarehouseSource(ExcelWarehouseSource copyFromSource) {
        super(copyFromSource);
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
    
    public Integer getDateRow() {
        return getParameter(DATE_ROW_PARAMETER_ID).getValueAsInteger();
    }
    
    public Integer getStartCol() {
        return getParameter(START_COL_PARAMETER_ID).getValueAsInteger();
    }
    
    public Integer getDateShift() {
        return getParameter(DATE_SHIFT_PARAMETER_ID).getValueAsInteger();
    }

    
    public EventType getEventType() {
        return getOutput().getEventType();
    }

    @Override
    public ExcelWarehouseSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new ExcelWarehouseSource(sourceId, this);
    }

    @Override
    public ExcelWarehouseSource copyOf() {
        return new ExcelWarehouseSource(this);
    }

    public static ExcelWarehouseSource newTemplate() {
        UUID sourceId = UUID.randomUUID();
        ExcelWarehouseSource wrhSource = new ExcelWarehouseSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        wrhSource.addParameter(Parameter.stringParameterWithIdAndName(USER_NAME_PARAMETER_ID, "Имя пользователя").required(true)
                .defaultValue(""));
        wrhSource.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, "Password").required(true)
                .defaultValue(""));
        wrhSource.addParameter(Parameter.stringParameterWithIdAndName(FILE_NAME_PARAMETER_ID, "Папка для Excel файлов").required(true)
                .description("Укажите полный путь к папке, включая имя сервера и протокол:"
                + "\" http://173.72.110.131:8080/WebDavServer/iPlast/Production/\". Наклонная черта в конце - обязательна.")
                .defaultValue("http://173.72.110.131:8080/WebDavServer/iPlast/Warehouse/"));
        wrhSource.addParameter(Parameter.integerParameterWithIdAndName(SHEET_INDEX_PARAMETER_ID, "Индекс таблицы").required(true)
                .description("Индекс электронной таблицы в данной рабочей книге.")
                .defaultValue(0));
        wrhSource.addParameter(Parameter.integerParameterWithIdAndName(INCREAMENT_PARAMETER_ID, "Отступ для групп").required(true)
                .description("Шаг отступа, который отражает группировки для элементов из первой колонки.")
                .defaultValue(1));
        wrhSource.addParameter(Parameter.integerParameterWithIdAndName(DATE_ROW_PARAMETER_ID, "Строка с датой").required(true)
                .description("Номер строки с датой, уменьшенный на 1.")
                .defaultValue(8));
        wrhSource.addParameter(Parameter.integerParameterWithIdAndName(START_COL_PARAMETER_ID, "Стартовая колонка").required(true)
                .description("Номер колонки, с которой начинаются данные по складам, уменьшенный на 1.")
                .defaultValue(2));
        wrhSource.addParameter(Parameter.integerParameterWithIdAndName(DATE_SHIFT_PARAMETER_ID, "Кол колонок с данными").required(true)
                .description("Количество колонок с данными для каждой даты.")
                .defaultValue(4));

        wrhSource.setOutput(Output.outputWithId(1).setName("Map"));

        return wrhSource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        validate();

        return new CompiledExcelProductSource(this.copyOf());
    }

    private static class CompiledExcelProductSource implements CompiledExternalSource {
        private final ExcelWarehouseSource source;
        private volatile boolean running;

        public CompiledExcelProductSource(ExcelWarehouseSource source) {
            this.source = source;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) throws ProcessingException {
            // this needs to be atomic, both the check and set
            synchronized (this) {
                checkState(!running, "Source is already processing events. Cannot call processEvents again");
                running = true;
            }


            Map<String, Integer> cellIndexMap = Maps.newHashMap();
            cellIndexMap.put(ExcelSardineUtils.WAREHOUSE, 1);
            cellIndexMap.put(ExcelSardineUtils.ITEM, 1);
            cellIndexMap.put(ExcelSardineUtils.ITEM_TYPE, 1);
            cellIndexMap.put(ExcelSardineUtils.BEGINING, 0);
            cellIndexMap.put(ExcelSardineUtils.INCOMING, 1);
            cellIndexMap.put(ExcelSardineUtils.OUTGOING, 2);
            cellIndexMap.put(ExcelSardineUtils.ENDING, 3);

            try {
                String excelFile = source.getFileName();

                // Get all xml files
                Sardine sardine = SardineFactory.begin(source.getUserName(), source.getPassword());
                List<DavResource> resources = sardine.getResources(excelFile);

                for (DavResource res : resources) {
                    String url = res.getPath();
                            //getAbsoluteUrl();
                    if (res.isDirectory()) {
                        continue;
                    } else {
                        Map<String, String> props = res.getCustomProps();
                        if (props.get(ExcelSardineUtils.PROCESSED) == null) {
                            InputStream isData = sardine.get(url);
                            HSSFWorkbook book = new HSSFWorkbook(isData);

                            int index = 0;
                            int increament = source.getIncreament();
                            if (book.getNumberOfSheets() > index) {
                                if (increament == 0) {
                                    increament = ExcelSardineUtils.WRH_OUTLINE_INCREAMENT;
                                }
                                Sheet sheet = book.getSheetAt(index);
                                if (sheet == null) {
                                    continue;
                                }

                                // Iterate through the rows.
                                int splitRowNumber = 0;

                                if (sheet.getPaneInformation() != null && sheet.getPaneInformation().isFreezePane()) {
                                    splitRowNumber = sheet.getPaneInformation().getHorizontalSplitPosition();
                                }

                                Row dateRow = sheet.getRow(source.getDateRow());
                                int start = source.getStartCol();
                                int end = dateRow.getLastCellNum();

                                for (int dateShift = start; 
                                        dateShift < end - source.getDateShift(); 
                                        dateShift = dateShift + source.getDateShift()) {

                                    Map<String, Object> rowMap = Maps.newHashMap();
                                    
                                    rowMap.put(ExcelSardineUtils.DATE,
                                            ExcelSardineUtils.formatDate(dateRow.getCell(dateShift).getStringCellValue()));
                                    
                                    Sheet _sheet = book.getSheetAt(index);

                                    for (Iterator<Row> rowsIt = _sheet.rowIterator(); rowsIt.hasNext();) {
                                        Row row = rowsIt.next();
                                        if (row.getPhysicalNumberOfCells() <= 0 || row.getRowNum() < splitRowNumber) {
                                            continue;
                                        }

                                        Cell cell = row.getCell(1);
                                        int indent = cell.getCellStyle().getIndention();
                                        int absIndent = indent / increament;
//                                
                                        if (ExcelSardineUtils.processRowWrhSs(rowMap, row, cellIndexMap, absIndent, dateShift)) {

                                            processCellRange(rowMap, runtime);

                                            logger.log(Level.INFO, "Row data is {0}", rowMap);
                                        }
                                    }
                                }
                                props.put(ExcelSardineUtils.PROCESSED, ExcelSardineUtils.TRUE);
                                sardine.setCustomProps(url, props, null);
                            } else {
                                System.out.println("Property PROCESSED: " + props.get(ExcelSardineUtils.PROCESSED));
                                List<String> removeProps = new ArrayList<String>(1);
                                removeProps.add(ExcelSardineUtils.PROCESSED);

                                sardine.setCustomProps(url, null, removeProps);
                            }
                            break;
                        }
                    }
                }
            } catch (FileNotFoundException ex) {
                Exceptions.printStackTrace(ex);
            } catch (IOException ex) {
                Exceptions.printStackTrace(ex);
            }
        }

        void processCellRange(Map<String, Object> map, ProcessingRuntime runtime) {
            Thread thread = Thread.currentThread();
            
            if (!thread.isInterrupted() && running) {
                Event newEvent = new Event(map);                
                runtime.sendEventFromSource(newEvent, source);
            }
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }

    }
}
