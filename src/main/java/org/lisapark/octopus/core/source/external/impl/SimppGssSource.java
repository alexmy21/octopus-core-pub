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
import com.google.gson.Gson;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
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
import org.lisapark.octopus.util.gss.GssListUtils;
import org.openide.util.Exceptions;

/**
 *
 * @author Alex Mylnikov(alexmy@lisa-park.com)
 */

@Persistable
public class SimppGssSource extends ExternalSource {
 
    private final static java.util.logging.Logger logger 
            = java.util.logging.Logger.getLogger(GssSource.class.getName());

    private static final String DEFAULT_NAME = "SIMPP data from GSS";
    private static final String DEFAULT_DESCRIPTION = ""
//            "Читает все необходимые для симуляционного"
//            + " планирования производства данные из Google Spread Sheet и передает их СИПП процессору как строковые поля"
//            + " в формате XML."
            ;
    
    private static final int EMAIL_PARAMETER_ID             = 2;
    private static final int PASSWORD_PARAMETER_ID          = 3;
    private static final int WORK_BOOK_PARAMETER_ID         = 4;
    private static final int DATA_SHEET_PARAMETER_ID        = 5;
    private static final int PLAN_SHEET_PARAMETER_ID        = 6;
    private static final int RESOURCE_SHEET_PARAMETER_ID    = 7;
    private static final int DATA_NAME_PARAMETER_ID         = 8;
    private static final int PLAN_NAME_PARAMETER_ID         = 9;
    private static final int RESOURCE_NAME_PARAMETER_ID     = 10;
    
    private static final String DATA_NAME       = "data";
    private static final String PLAN_NAME       = "plan";
    private static final String RESOURCE_NAME   = "resource";

    private SimppGssSource(UUID sourceId, String name, String description) {
        super(sourceId, name, description);
    }

    private SimppGssSource(UUID sourceId, SimppGssSource copyFromSource) {
        super(sourceId, copyFromSource);        
    }

    private SimppGssSource(SimppGssSource copyFromSource) {
        super(copyFromSource);
    }

//    public String getUsername() {
//        return getParameter(USER_NAME_PARAMETER_ID).getValueAsString();
//    }

    public String getPassword() {
        return getParameter(PASSWORD_PARAMETER_ID).getValueAsString();
    }

    public String getEmail() {
        return getParameter(EMAIL_PARAMETER_ID).getValueAsString();
    }

    public String getDataSheet() {
        return getParameter(DATA_SHEET_PARAMETER_ID).getValueAsString();
    }

    public String getPlanSheet() {
        return getParameter(PLAN_SHEET_PARAMETER_ID).getValueAsString();
    }

    public String getResourceSheet() {
        return getParameter(RESOURCE_SHEET_PARAMETER_ID).getValueAsString();
    }

    public String getWorkBook() {
        return getParameter(WORK_BOOK_PARAMETER_ID).getValueAsString();
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
    public SimppGssSource newInstance() {
        UUID sourceId = UUID.randomUUID();

        return new SimppGssSource(sourceId, this);
    }

    @Override
    public SimppGssSource copyOf() {
        return new SimppGssSource(this);
    }

    public static SimppGssSource newTemplate() {
        UUID sourceId = UUID.randomUUID();
        SimppGssSource simppGss = new SimppGssSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        
        simppGss.addParameter(Parameter.stringParameterWithIdAndName(EMAIL_PARAMETER_ID, "User email: ")
                .defaultValue("demo@lisa-park.com"));
        simppGss.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, "Password: ")
                .defaultValue("isasdemo"));
        simppGss.addParameter(Parameter.stringParameterWithIdAndName(WORK_BOOK_PARAMETER_ID, "Spreadsheet name: ")
                .defaultValue("ProductionDemo")
                .required(true));
        
        simppGss.addParameter(Parameter.stringParameterWithIdAndName(DATA_SHEET_PARAMETER_ID,
                "Data sheet:").defaultValue(DATA_NAME).required(true));
        simppGss.addParameter(Parameter.stringParameterWithIdAndName(PLAN_SHEET_PARAMETER_ID,
                "Plan sheet:").defaultValue(PLAN_NAME).required(true));
        simppGss.addParameter(Parameter.stringParameterWithIdAndName(RESOURCE_SHEET_PARAMETER_ID,
                "Resource sheet:").defaultValue(RESOURCE_NAME).required(true));
        
        simppGss.addParameter(Parameter.stringParameterWithIdAndName(DATA_NAME_PARAMETER_ID,
                "Data field name:").defaultValue(DATA_NAME).required(true));
        simppGss.addParameter(Parameter.stringParameterWithIdAndName(PLAN_NAME_PARAMETER_ID,
                "Plan field name:").defaultValue(PLAN_NAME).required(true));
        simppGss.addParameter(Parameter.stringParameterWithIdAndName(RESOURCE_NAME_PARAMETER_ID,
                "Resource field name:").defaultValue(RESOURCE_NAME).required(true));

        simppGss.setOutput(Output.outputWithId(1).setName("DATA_OUT"));
        
        simppGss.addAttributeList();

        return simppGss;
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

        private final SimppGssSource source;
        private volatile boolean running;

        public CompiledExcelWebDavDataSource(SimppGssSource source) {
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
                GssListUtils gssDataList = new GssListUtils(source.getName(), source.getWorkBook(),
                        source.getDataSheet(), source.getEmail(), source.getPassword());
                gssDataList.loadSheet();
                List<Map<String, Object>> data;
                data = gssDataList.endOfSheetQuery(null); 
                
                logger.log(Level.INFO, "data ==> {0}", data.toString());
                
                GssListUtils gssPlanList = new GssListUtils(source.getName(), source.getWorkBook(),
                        source.getPlanSheet(), source.getEmail(), source.getPassword());
                gssPlanList.loadSheet();
                List<Map<String, Object>> plan;
                plan = gssPlanList.endOfSheetQuery(null);
                
                logger.log(Level.INFO, "plan ==> {0}", plan.toString());

                GssListUtils gssResourceList = new GssListUtils(source.getName(), source.getWorkBook(),
                        source.getResourceSheet(), source.getEmail(), source.getPassword());
                gssResourceList.loadSheet();
                List<Map<String, Object>> resource;
                resource = gssResourceList.endOfSheetQuery(null);
                
                logger.log(Level.INFO, "resource ==> {0}", resource.toString());
                
                Map<String, String> map = Maps.newHashMap(); 
                 
                Gson gson = new Gson();
                
                String gsonData = gson.toJson(data);
                map.put(source.getDataName(), gsonData);
                
                String gsonPlan = gson.toJson(plan);
                map.put(source.getPlanName(), gsonPlan);
                
                String gsonResource = gson.toJson(resource);
                map.put(source.getResourceName(), gsonResource);

                processGssData(map, runtime);
              
            } catch (ServiceException ex) {
                Exceptions.printStackTrace(ex);
            } catch (IOException ex) {
                Exceptions.printStackTrace(ex);
            } 
        }

        void processGssData(Map<String, String> map, ProcessingRuntime runtime){
            Thread thread = Thread.currentThread();
            EventType eventType = source.getEventType();
            
            if(!thread.isInterrupted() && running && (map != null)) {               
                Event newEvent = createEventFromGssData(map, eventType);
                runtime.sendEventFromSource(newEvent, source);
            }
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }

        Event createEventFromGssData(Map<String, String> map, EventType eventType){
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