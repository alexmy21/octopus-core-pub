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
package org.lisapark.octopus.core.processor.impl;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
import org.lisapark.octopus.ProgrammerException;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.runtime.ProcessorContext;

import java.util.Map;
import java.util.UUID;
import org.lisapark.octopus.core.processor.CompiledProcessor;
import org.lisapark.octopus.core.processor.Processor;
import org.lisapark.octopus.core.processor.ProcessorInput;
import org.lisapark.octopus.core.processor.ProcessorOutput;

/**
 * This {@link Processor} is used to adding two inputs together and producing an output.
 * <p/>
 * Addition is a mathematical operation that represents combining collections of objects together into a larger
 * collection. It is signified by the plus sign (+).
 *
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public class SimppChocoProcessor extends Processor<Void> {
    private static final String DEFAULT_NAME = "СИМПП Choco";
    private static final String DEFAULT_DESCRIPTION = "СИмуляционное Моделирование Планирования Производства";

    /**
     * Addition takes two inputs
     */
    private static final int DATA_INPUT_ID          = 1;
    private static final int PLAN_INPUT_ID          = 2;
    private static final int MACHINE_INPUT_ID       = 3;
    private static final int PLAN_RANGE_INPUT_ID    = 4;
    private static final int MACHINE_RANGE_INPUT_ID = 5;
    
    private static final int OUTPUT_ID              = 1;

    protected SimppChocoProcessor(UUID id, String name, String description) {
        super(id, name, description);
    }

    protected SimppChocoProcessor(UUID id, SimppChocoProcessor additionToCopy) {
        super(id, additionToCopy);
    }

    protected SimppChocoProcessor(SimppChocoProcessor additionToCopy) {
        super(additionToCopy);
    }

    public ProcessorInput getFirstInput() {
        // there are two inputs for addition
        return getInputs().get(0);
    }

    public ProcessorInput getSecondInput() {
        // there are two inputs for addition
        return getInputs().get(1);
    }

    @Override
    public SimppChocoProcessor newInstance() {
        return new SimppChocoProcessor(UUID.randomUUID(), this);
    }

    @Override
    public SimppChocoProcessor copyOf() {
        return new SimppChocoProcessor(this);
    }

    @Override
    public CompiledProcessor<Void> compile() throws ValidationException {
        validate();

        // we copy all the inputs and output taking a "snapshot" of this processor so we are isolated of changes
        SimppChocoProcessor copy = copyOf();

        return new CompiledProductionUnitProcessor(copy);
    }

    /**
     * Returns a new {@link Addition} processor configured with all the appropriate
     * {@link org.lisapark.octopus.core.parameter.Parameter}s, {@link org.lisapark.octopus.core.Input}s and {@link org.lisapark.octopus.core.Output}.
     *
     * @return new {@link Addition}
     */
    public static SimppChocoProcessor newTemplate() {
        UUID processorId = UUID.randomUUID();
        SimppChocoProcessor unit = new SimppChocoProcessor(processorId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        // two double inputs
        ProcessorInput<String> firstInput = ProcessorInput.stringInputWithId(DATA_INPUT_ID).name("Raw materials").description("Raw materials for Production unit.").build();
        unit.addInput(firstInput);

        ProcessorInput<String> secondInput = ProcessorInput.stringInputWithId(MACHINE_INPUT_ID).name("Waste").description("Production waste.").build();
        unit.addInput(secondInput);
        
        ProcessorInput<String> thirdInput = ProcessorInput.stringInputWithId(PLAN_INPUT_ID).name("Products").description("Production output.").build();
        unit.addInput(thirdInput);

        unit.addJoin(firstInput, secondInput);
//        unit.addJoin(secondInput, thirdInput);

        // double output
        try {
            unit.setOutput(ProcessorOutput.stringOutputWithId(OUTPUT_ID).nameAndDescription("JournalRecord").attributeName("inventory"));
        } catch (ValidationException ex) {
            // this should NOT happen. It means we created the Addition with an invalid attribute name
            throw new ProgrammerException(ex);
        }

        return unit;
    }

    static class CompiledProductionUnitProcessor extends CompiledProcessor<Void> {
        private final String firstAttributeName;
        private final String secondAttributeName;

        protected CompiledProductionUnitProcessor(SimppChocoProcessor unit) {
            super(unit);

            firstAttributeName = unit.getFirstInput().getSourceAttributeName();
            secondAttributeName = unit.getSecondInput().getSourceAttributeName();
        }

        @Override
        public Object processEvent(ProcessorContext<Void> ctx, Map<Integer, Event> eventsByInputId) {
            Event firstEvent = eventsByInputId.get(DATA_INPUT_ID);
            Event secondEvent = eventsByInputId.get(PLAN_INPUT_ID);

            Double firstOperand = firstEvent.getAttributeAsDouble(firstAttributeName);
            Double secondOperand = secondEvent.getAttributeAsDouble(secondAttributeName);

            firstOperand = (firstOperand == null) ? 0 : firstOperand;
            secondOperand = (secondOperand == null) ? 0 : secondOperand;

            return firstOperand + secondOperand;
        }
    }
}
