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
import com.google.gson.Gson;
import java.util.Collection;
import java.util.HashMap;
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

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 * 
 * Calculates parameters for Simple Regression Model
 * All model parameters are packed in JSON and can be used 
 * to evaluate forecast.
 * 
 */
public class ForecastSRM extends Processor<Double> {
    
    private final static java.util.logging.Logger logger 
            = java.util.logging.Logger.getLogger(ForecastSRM.class.getName());
    
    private static final String DEFAULT_NAME = "Forecast SRM";
    private static final String DEFAULT_DESCRIPTION = "Calculate parameters for Simple linear Regressional Forecast Model (SRM)."
            + " It takes only one parameter - time series. The argument (x in the model: y = a + b*x) is an index from array {0, 1, 2, ..., N}.";
    
    private static final String DEFAULT_WINDOW_LENGTH_DESCRIPTION = "Number of data points to consider when performing the calculations.";
    
    private static final String ISE_DESCRIPTION     = "Standard error of the intercept estimate (ISE).";
    private static final String PPM_DESCRIPTION     = "Pearson's product moment correlation coefficient (PPM).";
    private static final String SSE_DESCRIPTION     = "Standard error of the slope estimate (SSE).";
    private static final String SCI_DESCRIPTION     = "Half-width of a 95% confidence interval for the slope estimate (SCI).";
    private static final String MSE_DESCRIPTION     = "Sum of squared errors divided by the degrees of freedom (MSE).";
    private static final String SLS_DESCRIPTION     = "Significance level of the slope (equiv) correlation (SLS).";
    
    private static final String DEFAULT_OUTPUT_DESCRIPTION = "Result is JSON object that contains:" + 
            " (1) Linear regression formula;"
            + " (2) list of accuracy parameters (ISE, PPM, SSE, SCI, MSE, SLS).";

    /**
     * LinearRegressionProcessor takes two inputs
     */

    private static final int INPUT_FIELD_NAME_PARAM_ID      = 2;
    private static final int WINDOW_LENGTH_PARAMETER_ID     = 3;
    
//    private static final int INTERSEPT_NAME_PARAM_ID        = 5; 
//    private static final int SLOPE_NAME_PARAM_ID            = 6;
    private static final int INDEX_FIELD_PARAM_ID           = 6;
    private static final int FORMULA_FIELD_PARAM_ID         = 7;
    
    private static final int ISE_NAME_PARAM_ID  = 8;
    private static final int PPM_NAME_PARAM_ID  = 9;
    private static final int SSE_NAME_PARAM_ID  = 10;
    private static final int SCI_NAME_PARAM_ID  = 11;
    private static final int MSE_NAME_PARAM_ID  = 12;
    private static final int SLS_NAME_PARAM_ID  = 13;
    
    
//    private static final String INTERSEPT_NAME      = "Intercept name: ";
//    private static final String SLOPE_NAME          = "Slope name: ";
    private static final String FORMULA_FIELD_NAME  = "Formula Field name: ";
    private static final String INDEX_FIELD_NAME    = "Index Field name: ";
    
    private static final String ISE_NAME        = "Intersept Std Error name: ";
    private static final String PPM_NAME        = "Pearson Prod Moment name: ";
    private static final String SSE_NAME        = "Slope Std Error name: ";
    private static final String SCI_NAME        = "Confidence Interval name: ";
    private static final String MSE_NAME        = "MSE name: ";
    private static final String SLS_NAME        = "SLS name: ";
    
    private static final int OUTPUT_ID          = 1;   

    protected ForecastSRM(UUID id, String name, String description) {
        super(id, name, description);
    }

    protected ForecastSRM(UUID id, ForecastSRM correlationToCopy) {
        super(id, correlationToCopy);
    }

    protected ForecastSRM(ForecastSRM correlationToCopy) {
        super(correlationToCopy);
    }

    
    public int getWindowLength() {
        return getParameter(WINDOW_LENGTH_PARAMETER_ID).getValueAsInteger();
    }
     
    /**
     * @return the coefficientA
     */
//    public String getInterseptName() {
//        return getParameter(INTERSEPT_NAME_PARAM_ID).getValueAsString();
//    }

    /**
     * @return the coefficientB
     */
//    public String getSlopeName() {
//        return getParameter(SLOPE_NAME_PARAM_ID).getValueAsString();
//    }

    public ProcessorInput getDataFieldName() {
        return getInputs().get(0);
    }

    
    private String getFormulaFieldName() {
        return getParameter(FORMULA_FIELD_PARAM_ID).getValueAsString();
    }

    private String getIndexFieldName() {
        return getParameter(INDEX_FIELD_PARAM_ID).getValueAsString();
    }

    private String getIseName() {
        return getParameter(ISE_NAME_PARAM_ID).getValueAsString();
    }

    private String getPpmName() {
        return getParameter(PPM_NAME_PARAM_ID).getValueAsString();
    }

    private String getSseName() {
        return getParameter(SSE_NAME_PARAM_ID).getValueAsString();
    }

    private String getSciName() {
        return getParameter(SCI_NAME_PARAM_ID).getValueAsString();
    }

    private String getMseName() {
        return getParameter(MSE_NAME_PARAM_ID).getValueAsString();
    }

    private String getSlsName() {
        return getParameter(SLS_NAME_PARAM_ID).getValueAsString();
    }

    @Override
    public ForecastSRM newInstance() {
        return new ForecastSRM(UUID.randomUUID(), this);
    }

    @Override
    public ForecastSRM copyOf() {
        return new ForecastSRM(this);
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
    public Memory<Double> createMemoryForProcessor(MemoryProvider memoryProvider) {
        return memoryProvider.createCircularBuffer(getWindowLength());
    }
    
    @Override
    public CompiledProcessor<Double> compile() throws ValidationException {
        validate();

        // we copy all the inputs and output taking a "snapshot" of this processor so we are isolated of changes
        ForecastSRM copy = copyOf();

        return new CompiledRegression(copy);
    }

    /**
     * 
     * @return 
     */
    public static ForecastSRM newTemplate() {
        UUID processorId = UUID.randomUUID();
        ForecastSRM regression = new ForecastSRM(processorId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        
        ProcessorInput<Double> inputFieldName = ProcessorInput.doubleInputWithId(INPUT_FIELD_NAME_PARAM_ID).name("Input field name: ")
                .description("Input data field name.").build();
        regression.addInput(inputFieldName);

        // Setting parameters
        //======================================================================
        regression.addParameter(
                Parameter.integerParameterWithIdAndName(WINDOW_LENGTH_PARAMETER_ID, "Time-window").
                        description(DEFAULT_WINDOW_LENGTH_DESCRIPTION).
                        defaultValue(10).required(true).
                        constraint(Constraints.integerConstraintWithMinimumAndMessage(1, "Sample size, should be greater than 0."))
        );
   
        regression.addParameter(
                Parameter.stringParameterWithIdAndName(FORMULA_FIELD_PARAM_ID, FORMULA_FIELD_NAME).
                        description("Forecast formula field name.").
                        defaultValue("formula").required(true)
        );
          
        regression.addParameter(
                Parameter.stringParameterWithIdAndName(INDEX_FIELD_PARAM_ID, INDEX_FIELD_NAME).
                        description("Time or Index field name.").
                        defaultValue("index").required(true)
        );
        
        regression.addParameter(
                Parameter.stringParameterWithIdAndName(ISE_NAME_PARAM_ID, ISE_NAME).
                        description(ISE_DESCRIPTION).
                        defaultValue("ISE").required(true)
        );

        regression.addParameter(
                Parameter.stringParameterWithIdAndName(PPM_NAME_PARAM_ID, PPM_NAME).
                        description(PPM_DESCRIPTION).
                        defaultValue("PPM").required(true)
        );
        
        regression.addParameter(
                Parameter.stringParameterWithIdAndName(SSE_NAME_PARAM_ID, SSE_NAME).
                        description(SSE_DESCRIPTION).
                        defaultValue("SSE").required(true)
        );
        
        regression.addParameter(
                Parameter.stringParameterWithIdAndName(SCI_NAME_PARAM_ID, SCI_NAME).
                        description(SCI_DESCRIPTION).
                        defaultValue("SCI").required(true)
        );
        
        regression.addParameter(
                Parameter.stringParameterWithIdAndName(MSE_NAME_PARAM_ID, MSE_NAME).
                        description(MSE_DESCRIPTION).
                        defaultValue("MSE").required(true)
        );
        
        regression.addParameter(
                Parameter.stringParameterWithIdAndName(SLS_NAME_PARAM_ID, SLS_NAME).
                        description(SLS_DESCRIPTION).
                        defaultValue("SLS").required(true)
        );
        
        // double output
        //======================================================================
        try {
            regression.setOutput(ProcessorOutput.doubleOutputWithId(OUTPUT_ID).name("Regression").attributeName("JSON")
                    .description(DEFAULT_OUTPUT_DESCRIPTION));
        } catch (ValidationException ex) {
            // this should NOT happen. It means we created the LinearRegressionProcessor with an invalid attribute name
            throw new ProgrammerException(ex);
        }

        return regression;
    }

    static class CompiledRegression extends CompiledProcessor<Double> {
        
        private final static java.util.logging.Logger logger 
            = java.util.logging.Logger.getLogger(CompiledRegression.class.getName());
        
//        private final String firstAttributeName;
        private final String dataFieldName;
        
        private final ForecastSRM forecastSRM;

        protected CompiledRegression(ForecastSRM forecastSRM) {
            super(forecastSRM);
            this.forecastSRM = forecastSRM;

//            firstAttributeName = regression.getFirstInput().getSourceAttributeName();
            dataFieldName = forecastSRM.getDataFieldName().getSourceAttributeName();
        }

        @Override
        public Object processEvent(ProcessorContext<Double> ctx, Map<Integer, Event> events) {
//            Event firstEvent = eventsByInputId.get(FIRST_INPUT_ID);
            Event event = events.get(INPUT_FIELD_NAME_PARAM_ID);

//            Double firstOperand = firstEvent.getAttributeAsDouble(firstAttributeName);
            Double secondOperand = event.getAttributeAsDouble(dataFieldName);
            
//            Map<String, Object> retMap = Maps.newHashMap();
            String json = null;
            
            if (secondOperand != null) {
                
                Memory<Double> processorMemory = ctx.getProcessorMemory();
                
                Double newValue = new Double(secondOperand);
                processorMemory.add(newValue);
               
                int arraySize = forecastSRM.getWindowLength(); 

                final Collection<Double> memoryItems = processorMemory.values();
                
                SimpleRegression simpleRegression = new SimpleRegression();
               
                if (memoryItems.size() >= arraySize) {

                    int i = 0;
                    for (Double memoryItem : memoryItems) {
                        simpleRegression.addData(i, memoryItem);
                        i++;
                    }
                    
                    simpleRegression.regress();
                    
                    HashMap<String, Object> map = Maps.newHashMap();
                    
                    String formula = simpleRegression.getIntercept() 
                            + "+(" + simpleRegression.getSlope() + "*"
                            + forecastSRM.getIndexFieldName() + ")";
                    
                    logger.log(Level.INFO, "Formula:{0}", formula);
                    
                    map.put(forecastSRM.getFormulaFieldName(), formula);
                    map.put(forecastSRM.getIseName(), simpleRegression.getInterceptStdErr());
                    map.put(forecastSRM.getPpmName(), simpleRegression.getR());
                    map.put(forecastSRM.getSseName(), simpleRegression.getSlopeStdErr());
                    map.put(forecastSRM.getSciName(), simpleRegression.getSlopeConfidenceInterval());
                    map.put(forecastSRM.getMseName(), simpleRegression.getMeanSquareError());
                    map.put(forecastSRM.getSlsName(), simpleRegression.getSignificance());
                    
                    Gson gson = new Gson();
                    json = gson.toJson(map);
                }
            } 
            
            String encodedJson = json;

logger.log(Level.INFO, "JSON: {0}", encodedJson);

            return encodedJson;
        }    
    }    
}
