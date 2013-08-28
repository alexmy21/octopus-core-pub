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
public class VectorAddition extends Processor<Void> {
    private static final String DEFAULT_NAME = "VectorAddition";
    private static final String DEFAULT_DESCRIPTION = "Add 2 operands";

    /**
     * Addition takes two inputs
     */
    private static final int FIRST_INPUT_ID = 1;
    private static final int SECOND_INPUT_ID = 2;
    private static final int OUTPUT_ID = 1;

    protected VectorAddition(UUID id, String name, String description) {
        super(id, name, description);
    }

    protected VectorAddition(UUID id, VectorAddition additionToCopy) {
        super(id, additionToCopy);
    }

    protected VectorAddition(VectorAddition additionToCopy) {
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
    public VectorAddition newInstance() {
        return new VectorAddition(UUID.randomUUID(), this);
    }

    @Override
    public VectorAddition copyOf() {
        return new VectorAddition(this);
    }

    @Override
    public CompiledProcessor<Void> compile() throws ValidationException {
        validate();

        // we copy all the inputs and output taking a "snapshot" of this processor so we are isolated of changes
        VectorAddition copy = copyOf();

        return new CompiledAddition(copy);
    }

    /**
     * Returns a new {@link Addition} processor configured with all the appropriate
     * {@link org.lisapark.octopus.core.parameter.Parameter}s, {@link org.lisapark.octopus.core.Input}s and {@link org.lisapark.octopus.core.Output}.
     *
     * @return new {@link Addition}
     */
    public static VectorAddition newTemplate() {
        UUID processorId = UUID.randomUUID();
        VectorAddition addition = new VectorAddition(processorId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        // two double inputs
        ProcessorInput<String> firstInput = ProcessorInput.stringInputWithId(FIRST_INPUT_ID).name("First Operand").description("First operand for addition").build();
        addition.addInput(firstInput);

        ProcessorInput<String> secondInput = ProcessorInput.stringInputWithId(SECOND_INPUT_ID).name("Second Operand").description("Second operand for addition").build();
        addition.addInput(secondInput);

        addition.addJoin(firstInput, secondInput);

        // double output
        try {
            addition.setOutput(ProcessorOutput.stringOutputWithId(OUTPUT_ID).nameAndDescription("Total").attributeName("sum"));
        } catch (ValidationException ex) {
            // this should NOT happen. It means we created the Addition with an invalid attribute name
            throw new ProgrammerException(ex);
        }

        return addition;
    }

    static class CompiledAddition extends CompiledProcessor<Void> {
        private final String firstAttributeName;
        private final String secondAttributeName;

        protected CompiledAddition(VectorAddition addition) {
            super(addition);

            firstAttributeName = addition.getFirstInput().getSourceAttributeName();
            secondAttributeName = addition.getSecondInput().getSourceAttributeName();
        }

        @Override
        public Object processEvent(ProcessorContext<Void> ctx, Map<Integer, Event> eventsByInputId) {
            Event firstEvent = eventsByInputId.get(FIRST_INPUT_ID);
            Event secondEvent = eventsByInputId.get(SECOND_INPUT_ID);

            Double firstOperand = firstEvent.getAttributeAsDouble(firstAttributeName);
            Double secondOperand = secondEvent.getAttributeAsDouble(secondAttributeName);

            firstOperand = (firstOperand == null) ? 0 : firstOperand;
            secondOperand = (secondOperand == null) ? 0 : secondOperand;

            return firstOperand + secondOperand;
        }
    }
}

