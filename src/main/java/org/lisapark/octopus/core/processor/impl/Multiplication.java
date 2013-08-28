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
public class Multiplication extends Processor<Void> {
//    private static final String DEFAULT_NAME = "Multiplication";
//    private static final String DEFAULT_DESCRIPTION = "Multiply 2 operands";
    
    private static final String DEFAULT_NAME = "MULT";
    private static final String DEFAULT_DESCRIPTION = "Multiplication";

    /**
     * Multiplication takes two inputs
     */
    private static final int FIRST_INPUT_ID = 1;
    private static final int SECOND_INPUT_ID = 2;
    private static final int OUTPUT_ID = 1;

    protected Multiplication(UUID id, String name, String description) {
        super(id, name, description);
    }

    protected Multiplication(UUID id, Multiplication multiplicationToCopy) {
        super(id, multiplicationToCopy);
    }

    protected Multiplication(Multiplication multiplicationToCopy) {
        super(multiplicationToCopy);
    }

    public ProcessorInput getFirstInput() {
        // there are two inputs for Multiplication
        return getInputs().get(0);
    }

    public ProcessorInput getSecondInput() {
        // there are two inputs for Multiplication
        return getInputs().get(1);
    }

    @Override
    public Multiplication newInstance() {
        return new Multiplication(UUID.randomUUID(), this);
    }

    @Override
    public Multiplication copyOf() {
        return new Multiplication(this);
    }

    @Override
    public CompiledProcessor<Void> compile() throws ValidationException {
        validate();

        // we copy all the inputs and output taking a "snapshot" of this processor so we are isolated of changes
        Multiplication copy = copyOf();

        return new CompiledMultiplication(copy);
    }

    /**
     * Returns a new {@link Multiplication} processor configured with all the appropriate
     * {@link org.lisapark.octopus.core.parameter.Parameter}s, {@link org.lisapark.octopus.core.Input}s and {@link org.lisapark.octopus.core.Output}.
     *
     * @return new {@link Multiplication}
     */
    public static Multiplication newTemplate() {
        UUID processorId = UUID.randomUUID();
        Multiplication multiplication = new Multiplication(processorId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        // two double inputs
        ProcessorInput<Double> firstInput = ProcessorInput.doubleInputWithId(FIRST_INPUT_ID).name("Operand 1").description("First operand.").build();
        multiplication.addInput(firstInput);

        ProcessorInput<Double> secondInput = ProcessorInput.doubleInputWithId(SECOND_INPUT_ID).name("Operand 2").description("Second operand.").build();
        multiplication.addInput(secondInput);

        multiplication.addJoin(firstInput, secondInput);

        // double output
        try {
            multiplication.setOutput(ProcessorOutput.doubleOutputWithId(OUTPUT_ID).nameAndDescription("Result").attributeName("Result"));
        } catch (ValidationException ex) {
            // this should NOT happen. It means we created the Multiplication with an invalid attribute name
            throw new ProgrammerException(ex);
        }

        return multiplication;
    }

    static class CompiledMultiplication extends CompiledProcessor<Void> {
        private final String firstAttributeName;
        private final String secondAttributeName;

        protected CompiledMultiplication(Multiplication Multiplication) {
            super(Multiplication);

            firstAttributeName = Multiplication.getFirstInput().getSourceAttributeName();
            secondAttributeName = Multiplication.getSecondInput().getSourceAttributeName();
        }

        @Override
        public Object processEvent(ProcessorContext<Void> ctx, Map<Integer, Event> eventsByInputId) {
            Event firstEvent = eventsByInputId.get(FIRST_INPUT_ID);
            Event secondEvent = eventsByInputId.get(SECOND_INPUT_ID);

            Double firstOperand = firstEvent.getAttributeAsDouble(firstAttributeName);
            Double secondOperand = secondEvent.getAttributeAsDouble(secondAttributeName);

            firstOperand = (firstOperand == null) ? 0 : firstOperand;
            secondOperand = (secondOperand == null) ? 0 : secondOperand;

            return firstOperand * secondOperand;
        }
    }
}