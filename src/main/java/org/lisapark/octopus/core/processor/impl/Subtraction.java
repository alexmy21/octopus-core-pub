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

import java.util.Map;
import java.util.UUID;
import org.lisapark.octopus.ProgrammerException;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.processor.CompiledProcessor;
import org.lisapark.octopus.core.processor.Processor;
import org.lisapark.octopus.core.processor.ProcessorInput;
import org.lisapark.octopus.core.processor.ProcessorOutput;
import org.lisapark.octopus.core.runtime.ProcessorContext;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class Subtraction  extends Processor<Void> {
    private static final String DEFAULT_NAME = "Subtraction";
    private static final String DEFAULT_DESCRIPTION = "Subtract seconf operand from first operand";
    
//    private static final String DEFAULT_NAME = "Вычитание";
//    private static final String DEFAULT_DESCRIPTION = "Вычитание второго операнда из первого";

    /**
     * Subtraction takes two inputs
     */
    private static final int FIRST_INPUT_ID = 1;
    private static final int SECOND_INPUT_ID = 2;
    private static final int OUTPUT_ID = 1;

    protected Subtraction(UUID id, String name, String description) {
        super(id, name, description);
    }

    protected Subtraction(UUID id, Subtraction subtractionToCopy) {
        super(id, subtractionToCopy);
    }

    protected Subtraction(Subtraction subtractionToCopy) {
        super(subtractionToCopy);
    }

    public ProcessorInput getFirstInput() {
        // there are two inputs for Subtraction
        return getInputs().get(0);
    }

    public ProcessorInput getSecondInput() {
        // there are two inputs for Subtraction
        return getInputs().get(1);
    }

    @Override
    public Subtraction newInstance() {
        return new Subtraction(UUID.randomUUID(), this);
    }

    @Override
    public Subtraction copyOf() {
        return new Subtraction(this);
    }

    @Override
    public CompiledProcessor<Void> compile() throws ValidationException {
        validate();

        // we copy all the inputs and output taking a "snapshot" of this processor so we are isolated of changes
        Subtraction copy = copyOf();

        return new CompiledSubtraction(copy);
    }

    /**
     * Returns a new {@link Subtraction} processor configured with all the appropriate
     * {@link org.lisapark.octopus.core.parameter.Parameter}s, {@link org.lisapark.octopus.core.Input}s and {@link org.lisapark.octopus.core.Output}.
     *
     * @return new {@link Subtraction}
     */
    public static Subtraction newTemplate() {
        UUID processorId = UUID.randomUUID();
        Subtraction subtraction = new Subtraction(processorId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        // two double inputs
        ProcessorInput<Double> firstInput = ProcessorInput.doubleInputWithId(FIRST_INPUT_ID).name("First operand").description("First operand.").build();
        subtraction.addInput(firstInput);

        ProcessorInput<Double> secondInput = ProcessorInput.doubleInputWithId(SECOND_INPUT_ID).name("Second operand").description("Second operand.").build();
        subtraction.addInput(secondInput);

        subtraction.addJoin(firstInput, secondInput);

        // double output
        try {
            subtraction.setOutput(ProcessorOutput.doubleOutputWithId(OUTPUT_ID).nameAndDescription("Result").attributeName("result"));
        } catch (ValidationException ex) {
            // this should NOT happen. It means we created the Subtraction with an invalid attribute name
            throw new ProgrammerException(ex);
        }

        return subtraction;
    }

    static class CompiledSubtraction extends CompiledProcessor<Void> {
        private final String firstAttributeName;
        private final String secondAttributeName;

        protected CompiledSubtraction(Subtraction subtraction) {
            super(subtraction);

            firstAttributeName = subtraction.getFirstInput().getSourceAttributeName();
            secondAttributeName = subtraction.getSecondInput().getSourceAttributeName();
        }

        @Override
        public Object processEvent(ProcessorContext<Void> ctx, Map<Integer, Event> eventsByInputId) {
            Event firstEvent = eventsByInputId.get(FIRST_INPUT_ID);
            Event secondEvent = eventsByInputId.get(SECOND_INPUT_ID);

            Double firstOperand = firstEvent.getAttributeAsDouble(firstAttributeName);
            Double secondOperand = secondEvent.getAttributeAsDouble(secondAttributeName);

            firstOperand = (firstOperand == null) ? 0 : firstOperand;
            secondOperand = (secondOperand == null) ? 0 : secondOperand;

            return firstOperand - secondOperand;
        }
    }
}