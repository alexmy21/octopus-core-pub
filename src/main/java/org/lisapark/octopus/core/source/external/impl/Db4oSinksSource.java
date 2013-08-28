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

import com.db4o.ObjectContainer;
import com.db4o.ObjectServer;
import com.db4o.cs.Db4oClientServer;
import static com.google.common.base.Preconditions.checkState;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.lisapark.octopus.core.Output;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.ProcessingException;
import org.lisapark.octopus.core.ProcessorBean;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.event.EventType;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.ProcessingRuntime;
import org.lisapark.octopus.core.sink.external.ExternalSink;
import org.lisapark.octopus.core.source.external.CompiledExternalSource;
import org.lisapark.octopus.core.source.external.ExternalSource;
import org.lisapark.octopus.repository.RepositoryException;
import org.lisapark.octopus.repository.db4o.OctopusDb4oRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is an {@link ExternalSource} that is used to access object databases. It can be configured with
 * a db4o Url, username, password, and a query to execute.
 * <p/>
 * Currently, the source uses the {@link org.lisapark.octopus.core.Output#getEventType()} to get the names of the
 * columns and types of the columns, but it will probably be changed in the future to support a mapper that takes
 * a {@link ResultSet} and produces an {@link Event}.
 *
 * @author dave sinclair(david.sinclair@lisa-park.com)
 * @author alex mylnikov(alexmy@lisa-park.com)
 */
@Persistable
public class Db4oSinksSource extends ExternalSource {
    
    static final Logger LOG = LoggerFactory.getLogger(Db4oSinksSource.class);
    
    private static final String DEFAULT_NAME        = "Db4o Sinks Source";
    private static final String DEFAULT_DESCRIPTION = "Access to Octopus Sink processors in db4o using query.";

    private static final int URL_PARAMETER_ID           = 1;
    private static final int USER_NAME_PARAMETER_ID     = 2;
    private static final int PASSWORD_PARAMETER_ID      = 3;
    private static final int PORT_PARAMETER_ID          = 4;
    private static final int QUERY_PARAMETER_ID         = 5;

    private Db4oSinksSource(UUID sourceId, String name, String description) {
        super(sourceId, name, description);
    }

    private Db4oSinksSource(UUID sourceId, Db4oSinksSource copyFromSource) {
        super(sourceId, copyFromSource);
    }

    private Db4oSinksSource(Db4oSinksSource copyFromSource) {
        super(copyFromSource);
    }

    public String getUrl() {
        return getParameter(URL_PARAMETER_ID).getValueAsString();
    }

    public String getUsername() {
        return getParameter(USER_NAME_PARAMETER_ID).getValueAsString();
    }

    public String getPassword() {
        return getParameter(PASSWORD_PARAMETER_ID).getValueAsString();
    }

    public String getQuery() {
        return getParameter(QUERY_PARAMETER_ID).getValueAsString();
    }

    public Integer getPort() {
        return getParameter(PORT_PARAMETER_ID).getValueAsInteger();
    }

    public EventType getEventType() {
        return getOutput().getEventType();
    }

    @Override
    public Db4oSinksSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new Db4oSinksSource(sourceId, this);
    }

    @Override
    public Db4oSinksSource copyOf() {
        return new Db4oSinksSource(this);
    }

    public static Db4oSinksSource newTemplate() {
        UUID sourceId = UUID.randomUUID();
        Db4oSinksSource db4o = new Db4oSinksSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        db4o.addParameter(Parameter.stringParameterWithIdAndName(URL_PARAMETER_ID, "Db4oURL")
                .defaultValue("localhost")
                .required(true));
        db4o.addParameter(Parameter.stringParameterWithIdAndName(USER_NAME_PARAMETER_ID, "Db4oUserName"));
        db4o.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, "Db4oPassword:"));
        
        db4o.addParameter(Parameter.integerParameterWithIdAndName(PORT_PARAMETER_ID, "Db4oPrort:")
                .defaultValue(7777)
                .required(true));
        db4o.addParameter(Parameter.stringParameterWithIdAndName(QUERY_PARAMETER_ID, "Query:")
                .defaultValue("")
                .required(false));

        db4o.setOutput(Output.outputWithId(1).setName("ProcJSON:"));

        return db4o;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        validate();

        return new CompiledDb4oQuerySource(this.copyOf());
    }

    private static class CompiledDb4oQuerySource implements CompiledExternalSource {
        
        private final Db4oSinksSource source;
        private volatile boolean running;

        public CompiledDb4oQuerySource(Db4oSinksSource source) {
            this.source = source;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) throws ProcessingException {
            // this needs to be atomic, both the check and set
            synchronized (this) {
                checkState(!running, "Source is already processing events. Cannot call processEvents again");
                running = true;
            }            
            
            OctopusDb4oRepository container = new OctopusDb4oRepository();
            
            ObjectContainer client = Db4oClientServer.openClient(
                    source.getUrl(), 
                    source.getPort(), 
                    source.getUsername(), 
                    source.getPassword());
            
            Set<ExternalSink> sinkList;
            
            if(source.getQuery().trim().isEmpty()){
                sinkList = Sets.newHashSet(container.getAllExternalSinkTemplates().iterator());
            } else {
                try {
                    sinkList = container.getExternalSinkTemplates(source.getQuery());
                } catch (RepositoryException ex) {
                    LOG.info("Query exception: " + ex.getMessage());
                    sinkList = Sets.newHashSet(container.getAllExternalSinkTemplates().iterator());
                }
            }
            
            List<ProcessorBean> beanList = Lists.newArrayList();
            
            for(ExternalSink item : sinkList){
                String json = item.toJson();
                ProcessorBean bean = new Gson().fromJson(json, ProcessorBean.class);
                beanList.add(bean);
            }
            processProcList(beanList, runtime);
            
            if(client != null){
                client.close();
            }
        }

        void processProcList(List<ProcessorBean> procList, ProcessingRuntime runtime) {
            Thread thread = Thread.currentThread();
            EventType eventType = source.getEventType();

            int i = 0;
            while (!thread.isInterrupted() && running && i < procList.size()) {
                Event newEvent = createEventFromProcList(procList.get(i), eventType);
                runtime.sendEventFromSource(newEvent, source);
                i++;
            }
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }

        Event createEventFromProcList(ProcessorBean bean, EventType eventType){
            Map<String, Object> attributeValues = Maps.newHashMap();
            
            String json = new Gson().toJson(bean, ProcessorBean.class);
            
            attributeValues.put(ProcessorBean.NAME, bean.getName());
            attributeValues.put(ProcessorBean.AUTHOR_EMAIL, bean.getAuthorEmail());
            attributeValues.put(ProcessorBean.CLASS_NAME, bean.getClassName());
            attributeValues.put(ProcessorBean.DESCRIPTION, bean.getDescription());
            attributeValues.put(ProcessorBean.PROC_ID, bean.getProcId());
            attributeValues.put(ProcessorBean.PROC_TYPE, bean.getProcType());
            attributeValues.put(ProcessorBean.AUTHOR_EMAIL, bean.getAuthorEmail());
            attributeValues.put(ProcessorBean.PROC_JSON, json);
            
            LOG.info("Attribute MAP: " + attributeValues);

            return new Event(attributeValues);
        }
    }
}
