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
import com.googlecode.sardine.Sardine;
import com.googlecode.sardine.SardineFactory;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
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
import org.lisapark.octopus.util.xml.XmlUtils;
import org.openide.util.Exceptions;

/**
 * This class is an {@link ExternalSource} that is used to access relational
 * databases. It can be configured with a JDBC Url for the database, username,
 * password, Driver fully qualified class name, and a query to execute.
 * <p/>
 * Currently, the source uses the
 * {@link org.lisapark.octopus.core.Output#getEventType()} to get the names of
 * the columns and types of the columns, but it will probably be changed in the
 * future to support a mapper that takes a {@link ResultSet} and produces an
 * {@link Event}.
 *
 * @author dave sinclair(david.sinclair@lisa-park.com)
 * @author Alex Mylnikov(alexmy@lisa-park.com)
 */
@Persistable
public class SimppXmlWebDavSource extends ExternalSource {

    private static final String DEFAULT_NAME = "СИМПП XML файлы с Web DAV сервера";
    private static final String DEFAULT_DESCRIPTION = "Читает все необходимые для симуляционного"
            + " планирования производства XML файлы и передает их СИПП процессору как строковые поля.";
    private static final int USER_NAME_PARAMETER_ID     = 1;
    private static final int PASSWORD_PARAMETER_ID      = 2;
    private static final int DATA_URL_PARAMETER_ID      = 3;
    private static final int PLAN_URL_PARAMETER_ID      = 4;
    private static final int RESOURCE_URL_PARAMETER_ID  = 5;
    private static final int DATA_NAME_PARAMETER_ID     = 6;
    private static final int PLAN_NAME_PARAMETER_ID     = 7;
    private static final int RESOURCE_NAME_PARAMETER_ID = 8;
    
    private static final String DATA_NAME       = "data";
    private static final String PLAN_NAME       = "plan";
    private static final String RESOURCE_NAME   = "resource";

    private SimppXmlWebDavSource(UUID sourceId, String name, String description) {
        super(sourceId, name, description);
    }

    private SimppXmlWebDavSource(UUID sourceId, SimppXmlWebDavSource copyFromSource) {
        super(sourceId, copyFromSource);        
    }

    private SimppXmlWebDavSource(SimppXmlWebDavSource copyFromSource) {
        super(copyFromSource);
    }

    public String getUsername() {
        return getParameter(USER_NAME_PARAMETER_ID).getValueAsString();
    }

    public String getPassword() {
        return getParameter(PASSWORD_PARAMETER_ID).getValueAsString();
    }

    public String getDataUrl() {
        return getParameter(DATA_URL_PARAMETER_ID).getValueAsString();
    }

    public String getPlanUrl() {
        return getParameter(PLAN_URL_PARAMETER_ID).getValueAsString();
    }

    public String getResourceUrl() {
        return getParameter(RESOURCE_URL_PARAMETER_ID).getValueAsString();
    }

    public String getDataName() {
        return getParameter(DATA_NAME_PARAMETER_ID).getValueAsString();
    }

    public String getPlanName() {
        return getParameter(PLAN_NAME_PARAMETER_ID).getValueAsString();
    }

    public String getResourceName() {
        return getParameter(RESOURCE_NAME_PARAMETER_ID).getValueAsString();
    }

    public EventType getEventType() {
        return getOutput().getEventType();
    }

    @Override
    public SimppXmlWebDavSource newInstance() {
        UUID sourceId = UUID.randomUUID();

        return new SimppXmlWebDavSource(sourceId, this);
    }

    @Override
    public SimppXmlWebDavSource copyOf() {
        return new SimppXmlWebDavSource(this);
    }

    public static SimppXmlWebDavSource newTemplate() {
        UUID sourceId = UUID.randomUUID();
        SimppXmlWebDavSource webdav = new SimppXmlWebDavSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        
        webdav.addParameter(Parameter.stringParameterWithIdAndName(USER_NAME_PARAMETER_ID, "User name"));
        webdav.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, "Password"));

        webdav.addParameter(Parameter.stringParameterWithIdAndName(DATA_URL_PARAMETER_ID,
                "URL для файла Данных")
                .defaultValue("http://173.72.110.131:8080/WebDavServer/iPlast/Data.xml")
                .required(true));
        webdav.addParameter(Parameter.stringParameterWithIdAndName(PLAN_URL_PARAMETER_ID,
                "URL для файла Плана")
                .defaultValue("http://173.72.110.131:8080/WebDavServer/iPlast/Plan.xml")
                .required(true));
        webdav.addParameter(Parameter.stringParameterWithIdAndName(RESOURCE_URL_PARAMETER_ID,
                "URL для файла Ресурсов")
                .defaultValue("http://173.72.110.131:8080/WebDavServer/iPlast/Resources.xml")
                .required(true));

        webdav.addParameter(Parameter.stringParameterWithIdAndName(DATA_NAME_PARAMETER_ID,
                "Имя поля для Данных").defaultValue(DATA_NAME).required(true));
        webdav.addParameter(Parameter.stringParameterWithIdAndName(PLAN_NAME_PARAMETER_ID,
                "Имя поля для Плана").defaultValue(PLAN_NAME).required(true));
        webdav.addParameter(Parameter.stringParameterWithIdAndName(RESOURCE_NAME_PARAMETER_ID,
                "Имя поля для Ресурсов").defaultValue(RESOURCE_NAME).required(true));

        webdav.setOutput(Output.outputWithId(1).setName("XML_OUT"));
        
        webdav.addAttributeList();

        return webdav;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        validate();
        return new CompiledExcelWebDavDataSource(this.copyOf());
    }

    private void addAttributeList() {
        try {
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, DATA_NAME));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, PLAN_NAME));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, RESOURCE_NAME));
        } catch (ValidationException ex) {
            Exceptions.printStackTrace(ex);
        }
    }

    private static class CompiledExcelWebDavDataSource implements CompiledExternalSource {

        private final SimppXmlWebDavSource source;
        private volatile boolean running;

        public CompiledExcelWebDavDataSource(SimppXmlWebDavSource source) {
            this.source = source;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) throws ProcessingException {
            try {
                // this needs to be atomic, both the check and set
                synchronized (this) {
                    checkState(!running, "Source is already processing events. Cannot call processEvents again");
                    running = true;
                }

                Map<String, String> map = Maps.newHashMap();

                Sardine sardine = SardineFactory.begin(source.getUsername(), source.getPassword());
                
                InputStream isData = sardine.get(source.getDataUrl());                
                String xmlData = IOUtils.toString(isData, "UTF-8");
                map.put(source.getDataName(), XmlUtils.clean(xmlData));
                isData.close();

                InputStream isPlan = sardine.get(source.getPlanUrl());                
                String xmlPlan = IOUtils.toString(isPlan, "UTF-8");
                map.put(source.getPlanName(), XmlUtils.clean(xmlPlan));
                isPlan.close();

                InputStream isResource = sardine.get(source.getResourceUrl());
                String xmlResource = IOUtils.toString(isResource, "UTF-8");
                map.put(source.getResourceName(), XmlUtils.clean(xmlResource));
                isResource.close();

                processResultSet(map, runtime);
              
            } catch (IOException ex) {
                Exceptions.printStackTrace(ex);
            } 

        }

        void processResultSet(Map<String, String> map, ProcessingRuntime runtime){
            Thread thread = Thread.currentThread();
            EventType eventType = source.getEventType();
            
            if(!thread.isInterrupted() && running && (map != null)) {               
                Event newEvent = createEventFromJsonList(map, eventType);
                runtime.sendEventFromSource(newEvent, source);
            }
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }

        Event createEventFromJsonList(Map<String, String> map, EventType eventType){
            Map<String, Object> attributeValues = Maps.newHashMap();

            for (Attribute attribute : eventType.getAttributes()) {
                Class type = attribute.getType();
                String attributeName = attribute.getName();

                if (type == String.class) {
                    if (source.getDataName().equalsIgnoreCase(attributeName)
                            || source.getPlanName().equalsIgnoreCase(attributeName)
                            || source.getResourceName().equalsIgnoreCase(attributeName)) {

                        String value = map.get(attributeName);
                        attributeValues.put(attributeName, value);
                    }
                } else {
                    throw new IllegalArgumentException(String.format("Unknown attribute type %s", type));
                }
            }

            return new Event(attributeValues);
        }
    }
}