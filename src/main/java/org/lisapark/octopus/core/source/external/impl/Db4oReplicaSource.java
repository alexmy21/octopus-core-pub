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
import com.db4o.ObjectSet;
import com.db4o.cs.Db4oClientServer;
import com.db4o.drs.Replication;
import com.db4o.drs.ReplicationSession;
import com.db4o.drs.db4o.Db4oProviderFactory;
import com.db4o.drs.db4o.Db4oReplicationProvider;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lisapark.octopus.core.Output;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.ProcessingModel;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Attribute;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.event.EventType;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.ProcessingRuntime;
import org.lisapark.octopus.core.source.external.CompiledExternalSource;
import org.lisapark.octopus.core.source.external.ExternalSource;
import org.lisapark.octopus.repository.RepositoryException;
import org.lisapark.octopus.repository.db4o.OctopusDb4oRepository;
import org.openide.util.Exceptions;

/**
 *
 * @author alex (alexmy@lisa-park.com)
 */
@Persistable
public class Db4oReplicaSource extends ExternalSource {

    private static final String DEFAULT_NAME = "Db4o Replica";
    private static final String DEFAULT_DESCRIPTION = "Replicates Octopus designer local Db4o on Octopus server.";
    private static final int SOURCE_URL_PARAMETER_ID = 1;
    private static final int SOURCE_USER_NAME_PARAMETER_ID = 2;
    private static final int SOURCE_PASSWORD_PARAMETER_ID = 3;
    private static final int SOURCE_PORT_PARAMETER_ID = 4;
    private static final int SOURCE_QUERY_PARAMETER_ID = 5;
    private static final int SERVER_URL_PARAMETER_ID = 6;
    private static final int SERVER_USER_NAME_PARAMETER_ID = 7;
    private static final int SERVER_PASSWORD_PARAMETER_ID = 8;
    private static final int SERVER_PORT_PARAMETER_ID = 9;

    public Db4oReplicaSource(UUID id, String name, String description) {
        super(id, name, description);
    }

    private Db4oReplicaSource(UUID id, Db4oReplicaSource copyFromSource) {
        super(id, copyFromSource);
    }

    public Db4oReplicaSource(Db4oReplicaSource copyFromSource) {
        super(copyFromSource);
    }

    public String getSourceUrl() {
        return getParameter(SOURCE_URL_PARAMETER_ID).getValueAsString();
    }

    public String getSourceUserName() {
        return getParameter(SOURCE_USER_NAME_PARAMETER_ID).getValueAsString();
    }

    public String getSourcePassword() {
        return getParameter(SOURCE_PASSWORD_PARAMETER_ID).getValueAsString();
    }

    public Integer getSourcePort() {
        return getParameter(SOURCE_PORT_PARAMETER_ID).getValueAsInteger();
    }

    public String getSourceQuery() {
        return getParameter(SOURCE_QUERY_PARAMETER_ID).getValueAsString();
    }

    public String getServerUrl() {
        return getParameter(SERVER_URL_PARAMETER_ID).getValueAsString();
    }

    public String getServerUserName() {
        return getParameter(SERVER_USER_NAME_PARAMETER_ID).getValueAsString();
    }

    public String getServerPassword() {
        return getParameter(SERVER_PASSWORD_PARAMETER_ID).getValueAsString();
    }

    public Integer getServerPort() {
        return getParameter(SERVER_PORT_PARAMETER_ID).getValueAsInteger();
    }

    @Override
    public Db4oReplicaSource copyOf() {
        return new Db4oReplicaSource(this);
    }

    @Override
    public Db4oReplicaSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new Db4oReplicaSource(sourceId, this);
    }

    public static Db4oReplicaSource newTemplate() {
        UUID sourceId = UUID.randomUUID();

        Db4oReplicaSource replicaSource = new Db4oReplicaSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        replicaSource.setOutput(Output.outputWithId(1).setName("Output data"));

        replicaSource.addParameter(Parameter.stringParameterWithIdAndName(SOURCE_URL_PARAMETER_ID, "Source Db4oURL:")
                .description("Octopus server Db40 URL .")
                .defaultValue("localhost")
                .required(true));

        replicaSource.addParameter(Parameter.stringParameterWithIdAndName(SOURCE_USER_NAME_PARAMETER_ID, "Source Db4oUserName:")
                .description("Db4o database User name.")
                .defaultValue("designer")
                .required(true));

        replicaSource.addParameter(Parameter.stringParameterWithIdAndName(SOURCE_PASSWORD_PARAMETER_ID, "Source Db4oPassword:")
                .description("Password.")
                .defaultValue("designer")
                .required(true));

        replicaSource.addParameter(Parameter.integerParameterWithIdAndName(SOURCE_PORT_PARAMETER_ID, "Source Db4oPort:")
                .description("Port.")
                .defaultValue(7777)
                .required(true));

        replicaSource.addParameter(Parameter.stringParameterWithIdAndName(SOURCE_QUERY_PARAMETER_ID, "Query:")
                .description("Query.")
                .defaultValue("")
                .required(true));

        replicaSource.addParameter(Parameter.stringParameterWithIdAndName(SERVER_URL_PARAMETER_ID, "Server Db4oURL:")
                .description("Octopus server Db40 URL .")
                .defaultValue("localhost")
                .required(true));

        replicaSource.addParameter(Parameter.stringParameterWithIdAndName(SERVER_USER_NAME_PARAMETER_ID, "Server Db4oUserName:")
                .description("Octopus server Db4o database User name.")
                .defaultValue("server")
                .required(true));

        replicaSource.addParameter(Parameter.stringParameterWithIdAndName(SERVER_PASSWORD_PARAMETER_ID, "Server Db4oPassword:")
                .description("Octopus server Db4o Password.")
                .defaultValue("server")
                .required(true));

        replicaSource.addParameter(Parameter.integerParameterWithIdAndName(SERVER_PORT_PARAMETER_ID, "Server Db4oPort:")
                .description("Octopus serve Db4or Port.")
                .defaultValue(8989)
                .required(true));

        return replicaSource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        return new CompiledModelSource(copyOf());
    }

    class CompiledModelSource implements CompiledExternalSource {

        protected final Logger logger = Logger.getLogger(CompiledModelSource.class.getName());
        protected final Db4oReplicaSource source;
        protected volatile boolean running;
        protected Thread thread;
        private String DEFAULT_CLASS_NAME = "className";

        public CompiledModelSource(Db4oReplicaSource source) {
            this.source = source;
        }

        @Override
        public synchronized void startProcessingEvents(ProcessingRuntime runtime) {

            String surl = source.getSourceUrl();
            int sport = source.getSourcePort();
            String suid = source.getSourceUserName();
            String spsw = source.getSourcePassword();

            ObjectContainer sourceDb4o = Db4oClientServer.openClient(
                    surl, sport, suid, spsw);

            String turl = source.getServerUrl();
            int tport = source.getServerPort();
            String tuid = source.getServerUserName();
            String tpsw = source.getServerPassword();

            ObjectContainer targetDb4o = Db4oClientServer.openClient(
                    turl, tport, tuid, tpsw);

            EventType eventType = this.source.getOutput().getEventType();
            List<Attribute> attributes = eventType.getAttributes();
            
            Attribute attribute = null;
            
            if(attributes.size() > 0){
                attribute = attributes.get(0);
            } else {
                try {
                    attribute = Attribute.stringAttribute(DEFAULT_CLASS_NAME);
                } catch (ValidationException ex) {
                    Exceptions.printStackTrace(ex);
                }
            }
            
            OctopusDb4oRepository sourceRepo = new OctopusDb4oRepository();
            OctopusDb4oRepository targetRepo = new OctopusDb4oRepository();
            
            try {
                
                List<ProcessingModel> list 
                        = sourceRepo.getProcessingModelsByName(sourceDb4o, source.getSourceQuery());
                
                for (ProcessingModel model : list) {

                    try {
                        String className = model.getModelName();

                        model = sourceRepo.activateModel(model);
                      
                        targetRepo.saveProcessingModel(targetDb4o, model);

                        Event e = createEvent(className, attribute);
                        if (e != null) {
                            runtime.sendEventFromSource(e, this.source);
                        }
                    } catch (Exception e) {
                        // Do nothing, skip Class case
                    }

                }
                
                targetDb4o.close();
                sourceDb4o.close();
                
            } catch (RepositoryException ex) {
                Exceptions.printStackTrace(ex);
            }            
        }

        private Event createEvent(String className, Attribute attribute) {
            
            if (attribute != null) {
                try {
                    
                    Map attributeData = Maps.newHashMap();
                    attributeData.put(attribute.getName(), className);

                    return new Event(attributeData);
                } catch (Exception ex) {
                    logger.log(Level.INFO, className);
                }
            }
            return null;
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }
    }
}
