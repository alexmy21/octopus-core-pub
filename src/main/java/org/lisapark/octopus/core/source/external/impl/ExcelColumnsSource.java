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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.UUID;
import org.lisapark.octopus.core.Output;
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
import org.lisapark.octopus.util.Booleans;
import org.lisapark.octopus.util.jdbc.Connections;
import org.lisapark.octopus.util.jdbc.ResultSets;
import org.lisapark.octopus.util.jdbc.Statements;
import static com.google.common.base.Preconditions.checkState;

/**
 * Supports access to the Excel files saved in Web DAV Server.
 * File/files specified by query to the Web DAV Server database.
 * Spreadsheet from the Excel Work should have a region 
 * (top left - bottom right cells).
 * 
 * This source reads two columns from the region: first is interpreted as entry
 * names, second - as entry values.
 * 
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class ExcelColumnsSource extends ExternalSource {
    private static final String DEFAULT_NAME = "Запрорс к данным в MS Excel";
    private static final String DEFAULT_DESCRIPTION = "Выбираются данные из отмеченного блока ячеек в соответсвующей электронной таблице.";

    private static final int URL_PARAMETER_ID               = 1;
    private static final int USER_NAME_PARAMETER_ID         = 2;
    private static final int PASSWORD_PARAMETER_ID          = 3;
    private static final int DRIVER_PARAMETER_ID            = 4;
    private static final int QUERY_PARAMETER_ID             = 5;
    private static final int SPRAED_SHEET_PARAMETER_ID      = 6;
    private static final int TOP_LEFT_CELL_PARAMETER_ID     = 7;
    private static final int BOTTOM_RIGHT_CELL_PARAMETER_ID = 8;

    private ExcelColumnsSource(UUID sourceId, String name, String description) {
        super(sourceId, name, description);
    }

    private ExcelColumnsSource(UUID sourceId, ExcelColumnsSource copyFromSource) {
        super(sourceId, copyFromSource);
    }

    private ExcelColumnsSource(ExcelColumnsSource copyFromSource) {
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
    public ExcelColumnsSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new ExcelColumnsSource(sourceId, this);
    }

    @Override
    public ExcelColumnsSource copyOf() {
        return new ExcelColumnsSource(this);
    }

    public static ExcelColumnsSource newTemplate() {
        UUID sourceId = UUID.randomUUID();
        ExcelColumnsSource excel = new ExcelColumnsSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        excel.addParameter(Parameter.stringParameterWithIdAndName(URL_PARAMETER_ID, "URL").required(true));
        excel.addParameter(Parameter.stringParameterWithIdAndName(USER_NAME_PARAMETER_ID, "Имя Пользователя"));
        excel.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, "Пароль"));
        excel.addParameter(Parameter.stringParameterWithIdAndName(DRIVER_PARAMETER_ID, "Класс драйвера").required(true).
                constraint(Constraints.classConstraintWithMessage("%s is not a valid Driver Class")));
        excel.addParameter(Parameter.stringParameterWithIdAndName(QUERY_PARAMETER_ID, "Запрос").required(true));
        excel.addParameter(Parameter.stringParameterWithIdAndName(SPRAED_SHEET_PARAMETER_ID, "Эл. таблица")
                .required(true));
        excel.addParameter(Parameter.stringParameterWithIdAndName(TOP_LEFT_CELL_PARAMETER_ID, "Верхняя левая ячейка")
                .required(true));
        excel.addParameter(Parameter.stringParameterWithIdAndName(BOTTOM_RIGHT_CELL_PARAMETER_ID, "Нижняя правая ячейка")
                .required(true));

        excel.setOutput(Output.outputWithId(1).setName("Выходные данные"));

        return excel;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        validate();

        return new CompiledExcelColumnsSource(this.copyOf());
    }

    private static class CompiledExcelColumnsSource implements CompiledExternalSource {
        private final ExcelColumnsSource source;

        private volatile boolean running;

        public CompiledExcelColumnsSource(ExcelColumnsSource source) {
            this.source = source;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) throws ProcessingException {
            // this needs to be atomic, both the check and set
            synchronized (this) {
                checkState(!running, "Source is already processing events. Cannot call processEvents again");
                running = true;
            }

            Connection connection = getConnection(source.getDriverClass(), source.getUrl(), source.getUsername(), source.getPassword());
            Statement statement = null;
            ResultSet rs = null;
            try {
                statement = connection.createStatement();

                rs = statement.executeQuery(source.getQuery());
                processResultSet(rs, runtime);
            } catch (SQLException e) {
                throw new ProcessingException("Problem processing result set from database. Please check your settings.", e);

            } finally {
                ResultSets.closeQuietly(rs);
                Statements.closeQuietly(statement);
                Connections.closeQuietly(connection);
            }
        }

        void processResultSet(ResultSet rs, ProcessingRuntime runtime) throws SQLException {
            Thread thread = Thread.currentThread();
            EventType eventType = source.getEventType();

            while (!thread.isInterrupted() && running && rs.next()) {
                Event newEvent = createEventFromResultSet(rs, eventType);

                runtime.sendEventFromSource(newEvent, source);
            }
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }

        Connection getConnection(String className, String url, String userName, String password) throws ProcessingException {

            try {
                Class.forName(className);
            } catch (ClassNotFoundException e) {
                // this should never happen since the parameter is constrained
                throw new ProcessingException("Could not find JDBC Driver Class " + className, e);
            }

            Connection connection;

            try {
                if (userName == null && password == null) {
                    connection = DriverManager.getConnection(url);
                } else {
                    connection = DriverManager.getConnection(url, userName, password);
                }
            } catch (SQLException e) {
                throw new ProcessingException("Could not connect to database. Please check your settings.", e);
            }

            return connection;
        }

        Event createEventFromResultSet(ResultSet rs, EventType eventType) throws SQLException {
            Map<String, Object> attributeValues = Maps.newHashMap();

            for (Attribute attribute : eventType.getAttributes()) {
                Class type = attribute.getType();
                String attributeName = attribute.getName();

                if (type == String.class) {
                    String value = rs.getString(attributeName);
                    attributeValues.put(attributeName, value);

                } else if (type == Integer.class) {
                    int value = rs.getInt(attributeName);
                    attributeValues.put(attributeName, value);

                } else if (type == Short.class) {
                    short value = rs.getShort(attributeName);
                    attributeValues.put(attributeName, value);

                } else if (type == Long.class) {
                    long value = rs.getLong(attributeName);
                    attributeValues.put(attributeName, value);

                } else if (type == Double.class) {
                    double value = rs.getDouble(attributeName);
                    attributeValues.put(attributeName, value);

                } else if (type == Float.class) {
                    float value = rs.getFloat(attributeName);
                    attributeValues.put(attributeName, value);

                } else if (type == Boolean.class) {
                    String value = rs.getString(attributeName);
                    attributeValues.put(attributeName, Booleans.parseBoolean(value));
                } else {
                    throw new IllegalArgumentException(String.format("Unknown attribute type %s", type));
                }
            }

            return new Event(attributeValues);
        }
    }
}
