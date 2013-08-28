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
import org.lisapark.octopus.core.Output;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Attribute;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.event.EventType;
import org.lisapark.octopus.core.parameter.Constraints;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.ProcessingRuntime;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.lisapark.octopus.core.source.external.CompiledExternalSource;
import org.lisapark.octopus.core.source.external.ExternalSource;
import org.openide.util.Exceptions;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public class TestSource extends ExternalSource {

    private static final String DEFAULT_NAME = "Test data source";
    private static final String DEFAULT_DESCRIPTION = "Generate source data according to the provided attribute list.";

    private static final int NUMBER_OF_EVENTS_PARAMETER_ID = 1;

    public TestSource(UUID id, String name, String description) {
        super(id, name, description);
    }

    private TestSource(UUID id, TestSource copyFromSource) {
        super(id, copyFromSource);
    }

    public TestSource(TestSource copyFromSource) {
        super(copyFromSource);
    }

    public Integer getNumberOfEvents() {
        return getParameter(NUMBER_OF_EVENTS_PARAMETER_ID).getValueAsInteger();
    }

    @Override
    public TestSource copyOf() {
        return new TestSource(this);
    }

    @Override
    public TestSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new TestSource(sourceId, this);
    }

    public static TestSource newTemplate() {
        UUID sourceId = UUID.randomUUID();

        TestSource testSource = new TestSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        testSource.setOutput(Output.outputWithId(1).setName("Output"));
        testSource.addParameter(
                Parameter.integerParameterWithIdAndName(NUMBER_OF_EVENTS_PARAMETER_ID, "Number of Events").
                        description("Number of test events to generate.").
                        defaultValue(10).
                        constraint(Constraints.integerConstraintWithMinimumAndMessage(1,
                        "Number of events has to be greater than zero.")));
        return testSource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        return new CompiledTestSource(copyOf());
    }

    static class CompiledTestSource implements CompiledExternalSource {

        private final TestSource source;

        /**
         * Running is declared volatile because it may be access my different threads
         */
        private volatile boolean running;
        private long SLIEEP_TIME = 10L;

        public CompiledTestSource(TestSource source) {
            this.source = source;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) {
            Thread thread = Thread.currentThread();
            running = true;

            EventType eventType = source.getOutput().getEventType();
            List<Attribute> attributes = eventType.getAttributes();
            int numberEventsCreated = 0;

            while (!thread.isInterrupted() && running && numberEventsCreated < source.getNumberOfEvents()) {
                Event e = createEvent(attributes, numberEventsCreated++);

                runtime.sendEventFromSource(e, source);
                
                try {
                    Thread.sleep(SLIEEP_TIME);
                } catch (InterruptedException ex) {
                    Exceptions.printStackTrace(ex);
                }
            }
        }

        private Event createEvent(List<Attribute> attributes, int eventNumber) {
            Map<String, Object> attributeData = Maps.newHashMap();

            for (Attribute attribute : attributes) {
                attributeData.put(attribute.getName(), attribute.createSampleData(eventNumber));
            }

            return new Event(attributeData);
        }

        @Override
        public void stopProcessingEvents() {
            running = false;
        }
    }
}
