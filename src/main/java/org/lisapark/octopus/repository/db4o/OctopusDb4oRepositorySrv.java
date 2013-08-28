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
package org.lisapark.octopus.repository.db4o;

import com.db4o.ObjectContainer;
import com.db4o.cs.Db4oClientServer;
import com.db4o.cs.config.ClientConfiguration;
import com.db4o.ext.Db4oException;
import com.db4o.query.Predicate;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.lisapark.octopus.core.ProcessingModel;
import org.lisapark.octopus.repository.AbstractOctopusRepository;
import org.lisapark.octopus.repository.OctopusRepository;
import org.lisapark.octopus.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Set;
import org.lisapark.octopus.core.Input;
import org.lisapark.octopus.core.Output;
import org.lisapark.octopus.core.processor.Processor;
import org.lisapark.octopus.core.sink.external.ExternalSink;
import org.lisapark.octopus.core.source.Source;
import org.lisapark.octopus.core.source.external.ExternalSource;
import org.openide.util.Exceptions;


/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
public class OctopusDb4oRepositorySrv extends AbstractOctopusRepository implements OctopusRepository {

    private static final Logger LOG = LoggerFactory.getLogger(OctopusDb4oRepositorySrv.class);
    
    public static String   HOST    = "localhost";
    /**
     * the database file to be used by the server.
     */
    public static String   FILE    = "octopus.db4o";
    /**
     * the port to be used by the server.
     */
    public static int      PORT    = 4488;
    /**
     * the user name for access control.
     */
    public static String   USER    = "db4o";
    /**
     * the password for access control.
     */
    public static String   PASS    = "db4o";

    /**
     * This is the actual DB4O container that will do the work of persisting and retrieving objects.
     */
    private static ObjectContainer container = null;

    /**
     * @return the container
     */
    public static ObjectContainer getContainer() {
        return container;
    }

    /**
     * @param aContainer the container to set
     */
    public static void setContainer(ObjectContainer aContainer) {
        container = aContainer;
    }
    
    private final String fileName;

    public OctopusDb4oRepositorySrv(String fileName) {
        this.fileName   = fileName;
//        
//        OctopusDb4oRepository.container = ensureObjectContainerIsOpen();
//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            @Override
//            public void run() {
//                if (container != null) {
//                    container.close();
//                }
//            }
//        });
    }
   
    private ObjectContainer ensureObjectContainerIsOpen() {
        
        ObjectContainer client = Db4oClientServer.openClient(
                Db4oClientServer.newClientConfiguration(), "localhost", PORT, USER, PASS);
        
        return client;            
    }

    private void rollbackCloseAndClearObjectContainer() {
        if (getContainer() != null) {
            getContainer().rollback();

            getContainer().close();
            setContainer(null);
        }
    }

    @Override
    public synchronized void saveProcessingModel(ProcessingModel model) throws RepositoryException {
        checkArgument(model != null, "model cannot be null");

//        model.setLastSaved(DateTime.now());
        
        User user = new User();
        user.setName("Alex");
        user.setAge(66);

        ObjectContainer client = null;
        
        try {

            LOG.debug("Saving model {}...", model.getModelName());
            
            ClientConfiguration config = Db4oClientServer.newClientConfiguration();
            config.common().objectClass(ProcessingModel.class).cascadeOnUpdate(true);
            client = Db4oClientServer.openClient(
                    config, 
                    "localhost", PORT, USER, PASS);
            
//            client.store(user);
            client.ext().store(model, 20);
            client.commit();
            client.close();

            LOG.debug("Saving completed");
        } catch (Db4oException e) {            
            if(client != null){
                client.close();
            }
            throw new RepositoryException(e);
        }
    }
    
    class User {
        private String name;
        private int age;

        /**
         * @return the name
         */
        public String getName() {
            return name;
        }

        /**
         * @param name the name to set
         */
        public void setName(String name) {
            this.name = name;
        }

        /**
         * @return the age
         */
        public int getAge() {
            return age;
        }

        /**
         * @param age the age to set
         */
        public void setAge(int age) {
            this.age = age;
        }
        
    }

    @Override
    public synchronized List<ProcessingModel> getProcessingModelsByName(String name) throws RepositoryException {
        checkArgument(name != null, "name cannot be null");

        final String query;

        if (name.trim().length() == 0) {
            query = ".*";

        } else {
            // make sure that the search string always ends in a '*' 
            if (name.endsWith("*")) {
                // then change from the "regular" '*' to the regex friendly '.*'
                query = StringUtils.replace(name, "*", ".*");

            } else {
                query = StringUtils.replace(name, "*", ".*") + ".*";
            }
        }

        LOG.debug("Getting models like {}", query);

        ObjectContainer client = null;
        List<ProcessingModel> list = null;
        
        try {

            ClientConfiguration config = Db4oClientServer.newClientConfiguration();
            config.common().objectClass(ProcessingModel.class).cascadeOnUpdate(true);
            client = Db4oClientServer.openClient(
                    config, 
                    "localhost", PORT, USER, PASS);
            
            list = client.queryByExample(ProcessingModel.class);
            for(ProcessingModel model : list){
                activateModel(model);
            }

        } catch (Db4oException e) {
            throw new RepositoryException(e);
        } catch (IllegalStateException e) {
            // this is thrown if the class structure is incompatible with the current structure
            throw new RepositoryException(e);
        } finally {
            if (client != null) {
                client.close();
            }

            return list;
        }
    }

    @Override
    public ProcessingModel getProcessingModelByName(String name) throws RepositoryException {
        List<ProcessingModel> models = null;
        try {
            models = getProcessingModelsByName(name);
        } catch (RepositoryException ex) {
            Exceptions.printStackTrace(ex);
        }

        if (models == null || models.isEmpty()) {
            return null;
        } else {
            ProcessingModel model = models.get(0);
            
            activateModel(model);

            return model;
        }
    }
            
    /**
     * 
     * @param model 
     */
    private void activateModel(ProcessingModel model) {
        Set<ExternalSource> externalSources = model.getExternalSources();
        for (ExternalSource externalSource : externalSources) {
            Output output = externalSource.getOutput();            
        }

        Set<Processor> processors = model.getProcessors();
        for (Processor processor : processors) {
            List<? extends Input> inputs = processor.getInputs();
            for (Input input : inputs) {
                Source source = input.getSource();
            }
        }

        Set<ExternalSink> externalSinks = model.getExternalSinks();
        for (ExternalSink externalSink : externalSinks) {
            List<? extends Input> inputs = externalSink.getInputs();
            for (Input input : inputs) {
                Source source = input.getSource();
            }
        }       
    }  
    
    @Override
    public synchronized Set<ExternalSink> getExternalSinkTemplateByClassName(String name) throws RepositoryException {
        
        Set<ExternalSink> sinks = null;
        try {
            sinks = getExternalSinkTemplates(name);
        } catch (RepositoryException ex) {
            Exceptions.printStackTrace(ex);
        }

        if (sinks == null || sinks.isEmpty()) {
            return null;
        } else {
//            ExternalSink sink = sinks.get(0);            
            return sinks;
        }
    }

    @Override
    public synchronized Set<ExternalSource> getExternalSourceTemplateByClassName(String name) throws RepositoryException {
        
        Set<ExternalSource> sources = null;
        try {
            sources = getExternalSourceTemplates(name);
        } catch (RepositoryException ex) {
            Exceptions.printStackTrace(ex);
        }

        if (sources == null || sources.isEmpty()) {
            return null;
        } else {
//            ExternalSource source = sources.get(0);            
            return sources;
        }
    }

    @Override
    public synchronized Set<Processor> getProcessorTemplateByClassName(String name) throws RepositoryException {
        
        Set<Processor> processors = null;
        try {
            processors = getProcessorTemplates(name);
        } catch (RepositoryException ex) {
            Exceptions.printStackTrace(ex);
        }

        if (processors == null || processors.isEmpty()) {
            return null;
        } else {
//            Processor processor = processors.get(0);            
            return processors;
        }
    }

    private String createQuery(String name) {
        String query;
        if (name.trim().length() == 0) {
            query = ".*";

        } else {
            // make sure that the search string always ends in a '*' 
            if (name.endsWith("*")) {
                // then change from the "regular" '*' to the regex friendly '.*'
                query = StringUtils.replace(name, "*", ".*");

            } else {
                query = StringUtils.replace(name, "*", ".*") + ".*";
            }
        }
        return query;
    }
    
    /**
     * Extracts all external sinks that have name matched with name provided
     * @param name
     * @return
     * @throws RepositoryException 
     */
    @Override
    public synchronized Set<ExternalSink> getExternalSinkTemplates(String sinkName) throws RepositoryException {
        
        checkArgument(sinkName != null, "name cannot be null");

        final String query;
        
        query = createQuery(sinkName);

        LOG.debug("Getting sinks like {}", query);

        try {

            ObjectContainer client = Db4oClientServer.openClient(Db4oClientServer
                .newClientConfiguration(), "localhost", PORT, USER, PASS);
            
            Set<ExternalSink> set = Sets.newHashSet(client.query(new Predicate<ExternalSink>() {
                @Override
                public boolean match(ExternalSink sink) {
                    return sink.getName().matches(query);
                }
            }).iterator());

            client.commit();
            client.close();
            
            return set;
            
        } catch (Db4oException e) {
            throw new RepositoryException(e);
        } catch (IllegalStateException e) {
            // this is thrown if the class structure is incompatible with the current structure
            throw new RepositoryException(e);
        }
    }

    @Override
    public synchronized Set<ExternalSource> getExternalSourceTemplates(String name) throws RepositoryException {
        
        checkArgument(name != null, "name cannot be null");

        final String query;
        
        query = createQuery(name);

        LOG.debug("Getting sources like {}", query);

        try {
                      
            ObjectContainer client = Db4oClientServer.openClient(Db4oClientServer
                .newClientConfiguration(), "localhost", PORT, USER, PASS);
            
            Set<ExternalSource> set = Sets.newHashSet(client.query(new Predicate<ExternalSource>() {
                @Override
                public boolean match(ExternalSource source) {
                    return source.getName().matches(query);
                }
            }).iterator());

            client.commit();
            client.close();
            
            return set;
        } catch (Db4oException e) {
            throw new RepositoryException(e);
        } catch (IllegalStateException e) {
            // this is thrown if the class structure is incompatible with the current structure
            throw new RepositoryException(e);
        }
    }

    @Override
    public synchronized  Set<Processor> getProcessorTemplates(String name) throws RepositoryException {
        
        checkArgument(name != null, "name cannot be null");

        final String query;
        
        query = createQuery(name);

        LOG.debug("Getting processors like {}", query);

        try {
                      
            ObjectContainer client = Db4oClientServer.openClient(Db4oClientServer
                .newClientConfiguration(), "localhost", PORT, USER, PASS);
            
            Set<Processor> set = Sets.newHashSet(client.query(new Predicate<Processor>() {
                @Override
                public boolean match(Processor proc) {
                    return proc.getName().matches(query);
                }
            }).iterator());

            client.commit();
            client.close();
            
            return set;
        } catch (Db4oException e) {
            throw new RepositoryException(e);
        } catch (IllegalStateException e) {
            // this is thrown if the class structure is incompatible with the current structure
            throw new RepositoryException(e);
        }
    }
    
//    @Override
//    public void finalize(){
//        try {
//            container.close();
//            super.finalize();
//        } catch (Throwable ex) {
//            Exceptions.printStackTrace(ex);
//        }
//        
//    }
}
