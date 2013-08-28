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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import org.lisapark.octopus.core.Output;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.event.EventType;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.parameter.Parameter.Builder;
import org.lisapark.octopus.core.runtime.ProcessingRuntime;
import org.lisapark.octopus.core.source.external.CompiledExternalSource;
import org.lisapark.octopus.core.source.external.ExternalSource;

/**
 *
 * @author alex (alexmy@lisa-park.com)
 */
@Persistable
public class RedisQuittokenSource extends ExternalSource {

    private static final String DEFAULT_NAME = "Radis Quittoken Source";
    private static final String DEFAULT_DESCRIPTION = "Generates quit token to stop all Octopus models that are listening to the channel specified in corresponding Resin Publisher Sink of a given model.";
    private static final int QUIT_SIGNAL_NAME_PARAMETER_ID = 1;
    private static final int QUIT_SIGNAL_VALUE_PARAMETER_ID = 2;

    public RedisQuittokenSource(UUID id, String name, String description) {
        super(id, name, description);
    }

    private RedisQuittokenSource(UUID id, RedisQuittokenSource copyFromSource) {
        super(id, copyFromSource);
    }

    public RedisQuittokenSource(RedisQuittokenSource copyFromSource) {
        super(copyFromSource);
    }

    public String getQuitSignalName() {
        return getParameter(1).getValueAsString();
    }

    public String getQuitSignalValue() {
        return getParameter(2).getValueAsString();
    }

    @Override
    public RedisQuittokenSource copyOf() {
        return new RedisQuittokenSource(this);
    }

    @Override
    public RedisQuittokenSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new RedisQuittokenSource(sourceId, this);
    }

    public static RedisQuittokenSource newTemplate() {
        UUID sourceId = UUID.randomUUID();

        RedisQuittokenSource redisSource = new RedisQuittokenSource(sourceId, "Radis Quittoken Source", "Generates quit token to stop all Octopus models that are listening to the channel specified in corresponding Resin Publisher Sink of a given model.");
        redisSource.setOutput(Output.outputWithId(1).setName("Output data"));

        redisSource.addParameter(Parameter.stringParameterWithIdAndName(1, "Quit signal name:")
                .description("Quit signal/token name.")
                .defaultValue("attr")
                .required(true));

        redisSource.addParameter(Parameter.stringParameterWithIdAndName(2, "Quit signal value:")
                .description("Quit signal value.")
                .defaultValue("quit")
                .required(true));

        return redisSource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        return new CompiledRedisSource(copyOf());
    }

    class CompiledRedisSource implements CompiledExternalSource {

        protected final Logger logger = Logger.getLogger(CompiledRedisSource.class.getName());
        protected final RedisQuittokenSource source;
        protected volatile boolean running;
        protected Thread thread;

        public CompiledRedisSource(RedisQuittokenSource source) {
            this.source = source;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) {
            EventType eventType = this.source.getOutput().getEventType();
            List attributes = eventType.getAttributes();
            int numberEventsCreated = 0;

            Map attributeData = Maps.newHashMap();
            attributeData.put(this.source.getQuitSignalName(), this.source.getQuitSignalValue());

            runtime.sendEventFromSource(new Event(attributeData), this.source);
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }
    }
}
