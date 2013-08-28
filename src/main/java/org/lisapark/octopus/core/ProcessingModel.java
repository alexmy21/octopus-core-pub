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
package org.lisapark.octopus.core;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.lisapark.octopus.core.event.Attribute;
import org.lisapark.octopus.core.processor.Processor;
import org.lisapark.octopus.core.sink.external.ExternalSink;
import org.lisapark.octopus.core.source.external.ExternalSource;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.gson.Gson;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com) 03/06/2013 Alex Mylnikov
 * added toJson() method
 */
@Persistable
public class ProcessingModel implements Validatable {

    private String modelName;
    private DateTime lastSaved;
    private final Set<ExternalSource> externalSources = Sets.newHashSet();
    private final Set<Processor> processors = Sets.newHashSet();
    private final Set<ExternalSink> externalSinks = Sets.newHashSet();

    public ProcessingModel(String modelName) {
        checkArgument(modelName != null, "modelName cannot be null");
        this.modelName = modelName;
    }

    public void setModelName(String modelName) {
        checkArgument(modelName != null, "modelName cannot be null");
        this.modelName = modelName;
    }

    public DateTime getLastSaved() {
        return lastSaved;
    }

    public void setLastSaved(DateTime lastSaved) {
        checkArgument(lastSaved != null, "lastSaved cannot be null");
        this.lastSaved = lastSaved;
    }

    public void addExternalEventSource(ExternalSource source) {
        checkArgument(source != null, "source cannot be null");
        externalSources.add(source);
    }

    /**
     * Removes the specified
     * {@link org.lisapark.octopus.core.source.external.ExternalSource} from
     * this model. Doing so will remove any connections between this source and
     * any other sink or processor.
     *
     * @param source to remove from model
     */
    public void removeExternalEventSource(ExternalSource source) {
        checkArgument(externalSources.contains(source), "Model does not contain source " + source);

        for (ExternalSink candidateSink : externalSinks) {
            if (candidateSink.isConnectedTo(source)) {

                candidateSink.disconnect(source);
            }
        }

        for (Processor candidateProcessor : processors) {
            if (candidateProcessor.isConnectedTo(source)) {

                candidateProcessor.disconnect(source);
            }
        }

        externalSources.remove(source);
    }

    public void addExternalSink(ExternalSink sink) {
        checkArgument(sink != null, "sink cannot be null");
        externalSinks.add(sink);
    }

    /**
     * Removes the specified
     * {@link org.lisapark.octopus.core.sink.external.ExternalSink} from this
     * model.
     *
     * @param sink to remove from model
     */
    public void removeExternalEventSink(ExternalSink sink) {
        checkArgument(externalSinks.contains(sink), "Model does not contain sink " + sink);

        externalSinks.remove(sink);
    }

    public void addProcessor(Processor processor) {
        checkArgument(processor != null, "processor cannot be null");
        processors.add(processor);
    }

    /**
     * Removes the specified
     * {@link org.lisapark.octopus.core.processor.Processor} from this model.
     * Doing so will remove any connections between this processor and any other
     * sink or processor.
     *
     * @param processor to remove from model
     */
    public void removeProcessor(Processor processor) {
        checkArgument(processors.contains(processor), "Model does not contain sink " + processor);

        for (ExternalSink candidateSink : externalSinks) {
            if (candidateSink.isConnectedTo(processor)) {

                candidateSink.disconnect(processor);
            }
        }

        for (Processor candidateProcessor : processors) {
            if (candidateProcessor.isConnectedTo(processor)) {

                candidateProcessor.disconnect(processor);
            }
        }

        processors.remove(processor);
    }

    /**
     * Returns true if the specified source's {@link Attribute} is in use
     * anywhere in the current model.
     *
     * @param source of attribute
     * @param attribute to check usage of
     * @return true if the attribute of the specified source is in use
     */
    public boolean isExternalSourceAttributeInUse(ExternalSource source, Attribute attribute) {
        checkArgument(source != null, "source cannot be null");
        checkArgument(attribute != null, "attribute cannot be null");
        boolean inUse = false;

        for (ExternalSink candidateSink : externalSinks) {
            if (candidateSink.isConnectedTo(source)) {
                inUse = true;
            }
        }

        for (Processor candidateProcessor : processors) {
            if (candidateProcessor.isConnectedTo(source, attribute)) {

                inUse = true;
            }
        }

        return inUse;
    }

    /**
     * Returns true if the specified {@link ExternalSource} is in use anywhere
     * in the current model.
     *
     * @param source to check
     * @return true if the specified source is in use
     */
    public boolean isExternalSourceInUse(ExternalSource source) {
        checkArgument(source != null, "source cannot be null");
        boolean inUse = false;

        for (ExternalSink candidateSink : externalSinks) {
            if (candidateSink.isConnectedTo(source)) {
                inUse = true;
            }
        }

        for (Processor candidateProcessor : processors) {
            if (candidateProcessor.isConnectedTo(source)) {

                inUse = true;
            }
        }

        return inUse;
    }

    public Set<ExternalSource> getExternalSources() {
        return ImmutableSet.copyOf(externalSources);
    }

    public Set<ExternalSink> getExternalSinks() {
        return ImmutableSet.copyOf(externalSinks);
    }

    public Set<Processor> getProcessors() {
        return ImmutableSet.copyOf(processors);
    }

    public String getModelName() {
        return modelName;
    }

    /**
     * Validates the {@link #externalSources}, {@link #processors} and
     * {@link #externalSinks} for this model.
     *
     * @throws ValidationException thrown if any source, processor, or sink is
     * invalid.
     */
    @Override
    public void validate() throws ValidationException {
        // todo verify all connections??
        for (ExternalSource source : externalSources) {
            source.validate();
        }

        for (Processor<?> processor : processors) {
            processor.validate();
        }

        for (ExternalSink sink : externalSinks) {
            sink.validate();
        }
    }

    
    /**
     * 
     * @return 
     */
    public String toJson(){
        
        ModelBean modelBean = new ModelBean();
                
        modelBean.setModelName(getModelName());
        modelBean.setSources(buildSources());
        modelBean.setProcessors(buildProcessors());
        modelBean.setSinks(buildSinks());
        
        return new Gson().toJson(modelBean, ModelBean.class);
//        return new Gson().toJson(this, this.getClass());
    }

    private Set<String> buildSources() {        
        
        Set<ExternalSource> sourceset = getExternalSources();
        Set<String> sources = Sets.newHashSet();
       
        for(ExternalSource item : sourceset){            
           sources.add(item.toJson());
        }
        
        return sources;
    }

    private Set<String> buildProcessors() {

        Set<Processor> processorset = getProcessors();
        Set<String> _processors = Sets.newHashSet();
       
        for(Processor item : processorset){            
           _processors.add(item.toJson());
        }
        
        return _processors;
    }

    private Set<String> buildSinks() {
        
        Set<ExternalSink> sinkset = getExternalSinks();
        Set<String> sinks = Sets.newHashSet();
       
        for(ExternalSink item : sinkset){            
           sinks.add(item.toJson());
        }
        
        return sinks;
    }
}
