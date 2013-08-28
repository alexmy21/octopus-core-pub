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
import org.lisapark.octopus.util.Booleans;
import org.lisapark.octopus.util.gss.GssCellUtils;
import org.openide.util.Exceptions;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
@Persistable
public class GssSource extends ExternalSource {
//    private static final String DEFAULT_NAME        = "GssSource";
//    private static final String DEFAULT_DESCRIPTION = "Provides access to Google Spread Sheet source for events";
//    
    private static final String DEFAULT_NAME        = "Таблица Google";
    private static final String DEFAULT_DESCRIPTION = "Доступ к таблице Google для получения данных";
    
    private static final int USER_EMAIL_PARAMETER_ID        = 1;
    private static final int PASSWORD_PARAMETER_ID          = 2;
    private static final int WORK_BOOK_PARAMETER_ID         = 3;
    private static final int SPRAED_SHEET_PARAMETER_ID      = 4;
    private static final int TOP_LEFT_CELL_PARAMETER_ID     = 5;
    private static final int BOTTOM_RIGHT_CELL_PARAMETER_ID = 6;
    
     
    private final static java.util.logging.Logger logger 
            = java.util.logging.Logger.getLogger(GssSource.class.getName());

    private GssSource(UUID sourceId, String name, String description) {
        super(sourceId, name, description);
    }

    private GssSource(UUID sourceId, GssSource copyFromSource) {
        super(sourceId, copyFromSource);
    }

    private GssSource(GssSource copyFromSource) {
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
        getParameter(SPRAED_SHEET_PARAMETER_ID).setValue(driverClass);
    }

    public String getSpredSheet() {
        return getParameter(SPRAED_SHEET_PARAMETER_ID).getValueAsString();
    }

    @SuppressWarnings("unchecked")
    public void setTopLeftCell(String query) throws ValidationException {
        getParameter(TOP_LEFT_CELL_PARAMETER_ID).setValue(query);
    }

    public String getTopLeftCell() {
        return getParameter(TOP_LEFT_CELL_PARAMETER_ID).getValueAsString();
    }

    @SuppressWarnings("unchecked")
    public void setBottomRightCell(String query) throws ValidationException {
        getParameter(BOTTOM_RIGHT_CELL_PARAMETER_ID).setValue(query);
    }

    public String getBottomRightCell() {
        return getParameter(BOTTOM_RIGHT_CELL_PARAMETER_ID).getValueAsString();
    }

    public EventType getEventType() {
        return getOutput().getEventType();
    }

    @Override
    public GssSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new GssSource(sourceId, this);
    }

    @Override
    public GssSource copyOf() {
        return new GssSource(this);
    }

    public static GssSource newTemplate() {
        UUID sourceId = UUID.randomUUID();
        GssSource gssSource = new GssSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
       
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(USER_EMAIL_PARAMETER_ID, "email Пользователя"));
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, "Пароль"));
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(WORK_BOOK_PARAMETER_ID, "Имя Таблицы").required(true));
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(SPRAED_SHEET_PARAMETER_ID, "Имя Листа").required(true));
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(TOP_LEFT_CELL_PARAMETER_ID, "Верхняя левая ячейка").required(true));
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(BOTTOM_RIGHT_CELL_PARAMETER_ID, "Нижняя правая ячейка").required(true));

        gssSource.setOutput(Output.outputWithId(1).setName("Выходные данные"));

        return gssSource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        validate();

        return new CompiledGssSource(this.copyOf());
    }

    private static class CompiledGssSource implements CompiledExternalSource {
        private final GssSource source;

        private volatile boolean running;

        public CompiledGssSource(GssSource source) {
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
                GssCellUtils gssCell = new GssCellUtils(
                        source.getName(),
                        source.getWorkBook(),
                        source.getSpredSheet(),
                        source.getUseremail(),
                        source.getPassword(),
                        source.getTopLeftCell(),
                        source.getBottomRightCell());

                gssCell.loadSheet();
                List<Map<String, Object>> cellRange;

                cellRange = gssCell.getCellRangeAsList(source.getEventType().getAttributes());
                
//logger.log(     Level.INFO, "gssCell.getCellRangeAsList;{0}", cellRange);

                processCellRange(cellRange, runtime);
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
                Event newEvent = createEventFromCellRange(cellRange, eventType, count);
                count++;

                runtime.sendEventFromSource(newEvent, source);
            }
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }

        Event createEventFromCellRange(List<Map<String, Object>> cellRange, EventType eventType, int count) {
            Map<String, Object> attributeValues = Maps.newHashMap();

            for (Attribute attribute : eventType.getAttributes()) {
                Class type = attribute.getType();
                String attributeName = attribute.getName();

                if (type == String.class) {
                    String value = (String) cellRange.get(count).get(attributeName);
                    attributeValues.put(attributeName, value);

                } else if (type == Integer.class) {
                    int value = Integer.parseInt((String)cellRange.get(count).get(attributeName));
                    attributeValues.put(attributeName, value);

                } else if (type == Short.class) {
                    short value = Short.parseShort((String)cellRange.get(count).get(attributeName));
                    attributeValues.put(attributeName, value);

                } else if (type == Long.class) {
                    long value = Long.parseLong((String)cellRange.get(count).get(attributeName));
                    attributeValues.put(attributeName, value);

                } else if (type == Double.class) {
                    double value = Double.parseDouble((String)cellRange.get(count).get(attributeName));
                    attributeValues.put(attributeName, value);

                } else if (type == Float.class) {
                    float value = Float.parseFloat((String)cellRange.get(count).get(attributeName));
                    attributeValues.put(attributeName, value);

                } else if (type == Boolean.class) {
                    String value = (String) cellRange.get(count).get(attributeName);
                    attributeValues.put(attributeName, Booleans.parseBoolean(value));
                } else {
                    throw new IllegalArgumentException(String.format("Unknown attribute type %s", type));
                }
            }

            return new Event(attributeValues);
        }
    }
}
