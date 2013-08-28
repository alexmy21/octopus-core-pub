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

import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.lisapark.octopus.ProgrammerException;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.memory.Memory;
import org.lisapark.octopus.core.memory.MemoryProvider;
import org.lisapark.octopus.core.parameter.Constraints;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.processor.CompiledProcessor;
import org.lisapark.octopus.core.processor.Processor;
import org.lisapark.octopus.core.processor.ProcessorInput;
import org.lisapark.octopus.core.processor.ProcessorOutput;
import org.lisapark.octopus.core.runtime.ProcessorContext;
import org.lisapark.octopus.util.Pair;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class LinearRegressionProcessor extends Processor<Pair<Double, Double>> {
    
    private final static java.util.logging.Logger logger 
            = java.util.logging.Logger.getLogger(LinearRegressionProcessor.class.getName());
    
    private static final String DEFAULT_NAME = "LinearRegression";
    private static final String DEFAULT_DESCRIPTION = "Calculate parameters for Linear Regressional Model for two series ";
    private static final String DEFAULT_WINDOW_LENGTH_DESCRIPTION = "Number of data points to consider when performing the calculations.";
    private static final String DEFAULT_OUTPUT_DESCRIPTION = "Result is two parameters: " + 
            "intercept and slope";

    /**
     * LinearRegressionProcessor takes two inputs
     */
    private static final int FIRST_INPUT_ID = 1;
    private static final int SECOND_INPUT_ID = 2;
    private static final int WINDOW_LENGTH_PARAMETER_ID     = 3;
    
    private static final int A_COEFFICIENT_NAME_PARAM_ID    = 4; 
    private static final int B_COEFFICIENT_NAME_PARAM_ID    = 5;
    
    private static final String A_COEFFICIENT_NAME          = "Intercept name: ";
    private static final String B_COEFFICIENT_NAME          = "Slope name: ";
    
    private static final int OUTPUT_ID = 1;   

    protected LinearRegressionProcessor(UUID id, String name, String description) {
        super(id, name, description);
    }

    protected LinearRegressionProcessor(UUID id, LinearRegressionProcessor correlationToCopy) {
        super(id, correlationToCopy);
    }

    protected LinearRegressionProcessor(LinearRegressionProcessor correlationToCopy) {
        super(correlationToCopy);
    }

    
    public int getWindowLength() {
        return getParameter(WINDOW_LENGTH_PARAMETER_ID).getValueAsInteger();
    }

    @SuppressWarnings("unchecked")
    public void setWindowLength(int windowLength) throws ValidationException {
        getParameter(WINDOW_LENGTH_PARAMETER_ID).setValue(windowLength);
    }
    
    
    /**
     * @return the coefficientA
     */
    public String getCoefficientAname() {
        return getParameter(A_COEFFICIENT_NAME_PARAM_ID).getValueAsString();
    }

    /**
     * @param coefficientA the coefficientA to set
     */
    @SuppressWarnings("unchecked")
    public void setCoefficientAname(String coefficientA) throws ValidationException {
        getParameter(A_COEFFICIENT_NAME_PARAM_ID).setValue(coefficientA);
    }

    /**
     * @return the coefficientB
     */
    public String getCoefficientBname() {
        return getParameter(B_COEFFICIENT_NAME_PARAM_ID).getValueAsString();
    }

    /**
     * @param coefficientB the coefficientB to set
     */
    @SuppressWarnings("unchecked")
    public void setCoefficientBname(String coefficientB) throws ValidationException {
        getParameter(B_COEFFICIENT_NAME_PARAM_ID).setValue(coefficientB);
    }
    
    public ProcessorInput getFirstInput() {
        // there are two inputs for linearRegressionProcessor
        return getInputs().get(0);
    }

    public ProcessorInput getSecondInput() {
        // there are two inputs for linearRegressionProcessor
        return getInputs().get(1);
    }

    @Override
    public LinearRegressionProcessor newInstance() {
        return new LinearRegressionProcessor(UUID.randomUUID(), this);
    }

    @Override
    public LinearRegressionProcessor copyOf() {
        return new LinearRegressionProcessor(this);
    }
    
    /**
     * {@link LinearRegressionProcessor}s need memory to store the prior events that will be used 
     * to calculate parameters for Linear Regression. We
     * used a {@link MemoryProvider#createCircularBuffer(int)} to store this data.
     *
     * @param memoryProvider used to create LinearRegressionProcessor's memory
     * @return circular buffer
     */
    @Override
    public Memory<Pair<Double, Double>> createMemoryForProcessor(MemoryProvider memoryProvider) {
        return memoryProvider.createCircularBuffer(getWindowLength());
    }
    
    @Override
    public CompiledProcessor<Pair<Double, Double>> compile() throws ValidationException {
        validate();

        // we copy all the inputs and output taking a "snapshot" of this processor so we are isolated of changes
        LinearRegressionProcessor copy = copyOf();

        return new CompiledRegression(copy);
    }

    /**
     * Returns a new {@link LinearRegressionProcessor} processor configured with all the appropriate
     * {@link org.lisapark.octopus.core.parameter.Parameter}s, {@link org.lisapark.octopus.core.Input}s and {@link org.lisapark.octopus.core.Output}.
     *
     * @return new {@link LinearRegressionProcessor}
     */
    public static LinearRegressionProcessor newTemplate() {
        UUID processorId = UUID.randomUUID();
        LinearRegressionProcessor regression = new LinearRegressionProcessor(processorId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        
        regression.addParameter(
                Parameter.integerParameterWithIdAndName(WINDOW_LENGTH_PARAMETER_ID, "Time-window").
                        description(DEFAULT_WINDOW_LENGTH_DESCRIPTION).
                        defaultValue(10).required(true).
                        constraint(Constraints.integerConstraintWithMinimumAndMessage(1, "Sample size, should be greater than 0."))
        );
        
        regression.addParameter(
                Parameter.stringParameterWithIdAndName(A_COEFFICIENT_NAME_PARAM_ID, A_COEFFICIENT_NAME).
                        description("Name of the intercept.").
                        defaultValue("A").required(true)
        );
        
        regression.addParameter(
                Parameter.stringParameterWithIdAndName(B_COEFFICIENT_NAME_PARAM_ID, B_COEFFICIENT_NAME).
                        description("Name of the slope - coefficient in Y = A + B*X.").
                        defaultValue("B").required(true)
        );

        // two double inputs
        ProcessorInput<Double> firstInput = ProcessorInput.doubleInputWithId(FIRST_INPUT_ID).name("Sequence 1")
                .description("Sequence 1 data array.").build();
        regression.addInput(firstInput);

        ProcessorInput<Double> secondInput = ProcessorInput.doubleInputWithId(SECOND_INPUT_ID).name("Sequence 2")
                .description("Sequence 2 data array.").build();
        regression.addInput(secondInput);

//        correlation.addJoin(firstInput, secondInput);

        // double output
        try {
            regression.setOutput(ProcessorOutput.doubleOutputWithId(OUTPUT_ID).name("Regression").attributeName("LinearRegression")
                    .description(DEFAULT_OUTPUT_DESCRIPTION));
        } catch (ValidationException ex) {
            // this should NOT happen. It means we created the LinearRegressionProcessor with an invalid attribute name
            throw new ProgrammerException(ex);
        }

        return regression;
    }


    static class CompiledRegression extends CompiledProcessor<Pair<Double, Double>> {
        private final String firstAttributeName;
        private final String secondAttributeName;
        
        private LinearRegressionProcessor regression;
        private static final String INTRERSEPT  = "intersept";
        private static final String SLOPE       = "slope";

        protected CompiledRegression(LinearRegressionProcessor regression) {
            super(regression);
            this.regression = regression;

            firstAttributeName = regression.getFirstInput().getSourceAttributeName();
            secondAttributeName = regression.getSecondInput().getSourceAttributeName();
        }

        @Override
        public Object processEvent(ProcessorContext<Pair<Double, Double>> ctx, Map<Integer, Event> eventsByInputId) {
            Event firstEvent = eventsByInputId.get(FIRST_INPUT_ID);
            Event secondEvent = eventsByInputId.get(SECOND_INPUT_ID);

            Double firstOperand = firstEvent.getAttributeAsDouble(firstAttributeName);
            Double secondOperand = secondEvent.getAttributeAsDouble(secondAttributeName);
            
            Map<String, Object> retMap = Maps.newHashMap();
            
            if (firstOperand != null && secondOperand != null) {
                
                Memory<Pair<Double, Double>> processorMemory = ctx.getProcessorMemory();
                
                Pair<Double, Double> newPair = new Pair<Double, Double>(firstOperand, secondOperand);
                processorMemory.add(newPair);
               
                int arraySize = regression.getWindowLength(); 
                double[] first = new double[arraySize];
                double[] second = new double[arraySize];

                final Collection<Pair<Double, Double>> memoryItems = processorMemory.values();
                
                SimpleRegression simpleRegression = new SimpleRegression();
               
                if (memoryItems.size() >= arraySize) {

                    int i = 0;
                    for (Pair<Double, Double> memoryItem : memoryItems) {
                        simpleRegression.addData(memoryItem.getFirst(), memoryItem.getSecond());
                    }
                    
                    simpleRegression.regress();
                    
                    // y = a + bx; a - intersept; b - slope;
                    retMap.put(regression.getCoefficientAname(), simpleRegression.getIntercept());
                    retMap.put(regression.getCoefficientBname(), simpleRegression.getSlope());                    
                }
            }            
            
//            return getGssListEntryFromEvent(retMap);
            return retMap;
        }
        
        private String getGssListEntryFromEvent(Map<String, Object> map) {
            StringBuilder builder = new StringBuilder();  
            
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String entryValueString;
                String entryKeyString;

                // Use recursion to collect all entries from nested maps
                if (entry.getValue() instanceof Map) {
                    entryValueString = getGssListEntryFromEvent((Map<String, Object>) entry.getValue());
                    entryKeyString = "";
                } else {
                    entryValueString = entry.getValue().toString();
                    entryKeyString = entry.getKey().toString();
                }

                if (builder.length() > 0) {
                    builder.append(",");
                }
                if (entryKeyString.isEmpty()) {
                    builder.append(entryValueString);
                } else {
                    builder.append(entryKeyString).append("=").append(entryValueString);
                }
            }
            
logger.log(     Level.INFO, "builder.toString(): ==> {0}", builder.toString());
            
            return builder.toString();
        }
    }
}
