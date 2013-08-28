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
package org.lisapark.octopus.core.sink.external.impl;

import choco.Choco;
import choco.Options;
import choco.cp.model.CPModel;
import choco.cp.solver.CPSolver;
import choco.cp.solver.search.integer.valiterator.DecreasingDomain;
import choco.kernel.common.logging.ChocoLogging;
import choco.kernel.model.Model;
import choco.kernel.model.variables.integer.IntegerExpressionVariable;
import choco.kernel.model.variables.integer.IntegerVariable;
import choco.kernel.solver.ContradictionException;
import choco.kernel.solver.Solver;
import choco.kernel.solver.variables.integer.IntDomainVar;
import com.google.common.collect.ImmutableList;
import com.google.gdata.data.spreadsheet.ListEntry;
import com.google.gdata.data.spreadsheet.ListFeed;
import com.google.gdata.util.ServiceException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import org.lisapark.octopus.core.AbstractNode;
import org.lisapark.octopus.core.Input;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.SinkContext;
import org.lisapark.octopus.core.sink.external.CompiledExternalSink;
import org.lisapark.octopus.core.sink.external.ExternalSink;
import org.lisapark.octopus.core.source.Source;
import org.lisapark.octopus.util.gss.GssListUtils;
import org.lisapark.octopus.util.optimodel.ChocoOptiModel;
import org.lisapark.octopus.util.optimodel.ChocoOptiUtils;
import org.lisapark.octopus.util.optimodel.OptiBean;
import org.openide.util.Exceptions;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
@Persistable
public class ChocoSimppGssSink extends AbstractNode implements ExternalSink {
    
    private final static java.util.logging.Logger logger 
            = java.util.logging.Logger.getLogger(ChocoSimppGssSink.class.getName());

    private static final String DEFAULT_NAME        = "SIMPP: execute and save in GSS";
    private static final String DEFAULT_DESCRIPTION = "Accepts input data as string variables,"
            + " executes simulation and saves results in Google Spreadsheet.";
    private static final int USER_NAME_PARAMETER_ID             = 3;
    private static final int PASSWORD_PARAMETER_ID              = 2;
    private static final int EMAIL_PARAMETER_ID                 = 1;
    
    private static final int WORK_SHEET_PARAMETER_ID            = 4;
    private static final int DATA_SHEET_PARAMETER_ID            = 5;
    private static final int PLAN_SHEET_PARAMETER_ID            = 6;
    private static final int RESOURCE_SHEET_PARAMETER_ID        = 7;
    private static final int DATA_NAME_PARAMETER_ID             = 8;
    private static final int PLAN_NAME_PARAMETER_ID             = 9;
    private static final int RESOURCE_NAME_PARAMETER_ID         = 10;
    private static final int ATTRIBUTE_NAMES_PARAMETER_ID       = 11;
   
    private static final int LOW_VALUE_PARAMETER_ID             = 12;
    private static final int HIGH_VALUE_PARAMETER_ID            = 13;
    
    private static final int LOW_COST_PARAMETER_ID              = 14;
    private static final int HIGH_COST_PARAMETER_ID             = 15;
    
    private static final int OPTI_MODE_PARAMETER_ID             = 16;
    private static final int PLAN_TYPE_PARAMETER_ID             = 17;
    
    private static final int USER_FIELD_NAME_PARAMETER_ID       = 18;
    private static final int PASSWORD_FIELD_NAME_PARAMETER_ID   = 19;
    
    private static final int PRECISION_PARAMETER_ID             = 20;
    
    private static final int TIME_OUT_PARAMETER_ID              = 21;
    
    private static final int VARIABLE_OPTION_PARAMETER_ID       = 22;
    
    public static final String USER_NAME            = "username";
    public static final String PASSWORD_NAME        = "password";
    public static final String DATA_NAME            = "data";
    public static final String PLAN_NAME            = "plan";
    public static final String RESOURCE_NAME        = "resource";
    public static final String OPTIMODE_NAME        = "optimode";    
     
    public static final int PLAN_TYPE_AGGRESSIVE    = ChocoOptiModel.PLAN_TYPE_AGGRESSIVE;
    public static final int PLAN_TYPE_BALANCED      = ChocoOptiModel.PLAN_TYPE_BALANCED;
    public static final int PLAN_TYPE_CAUTIOUS      = ChocoOptiModel.PLAN_TYPE_CAUTIOUS;
    
    public static final String VARIABLE_OPTION      = Options.V_BLIST;
    
    public static final String ATTRIBUTE_NAMES  
            = "PROD=productid,"
            + "PROCESS_STEP=technologystepnumber,"
            + "PROCESSOR=machineid,"
            + "PROCESS_COST=costperunit,"            
            + "UNIT_VALUE=unitvalue,"
            + "PROD_VALUE=prodvalue,"
            + "LOW_BOUND=lowbound,"
            + "UPPER_BOUND=upperbound,"
            + "PLAN_VALUE=planvalue,"
            + "RESOURCE=machineid,"
            + "RESOURCE_VALUE=resourcevalue,"
            + "FIXED=fixed"
            ;
    
    public static final int LOW_VALUE           = 0;
    public static final int HIGH_VALUE          = 10000000;
    public static final int LOW_COST            = 0;
    public static final int HIGH_COST           = 10000000;
    
    public static final int PRECISIONT_DEFAULT  = 1;
    public static final int TIME_OUT_DEFAULT    = 30000;
    
    private static final String DEFAULT_INPUT   = "Input data";    
        
    private Input<Event> input;

    private ChocoSimppGssSink(UUID id, String name, String description) {
        super(id, name, description);
        input = Input.eventInputWithId(1);
        input.setName(DEFAULT_INPUT);
        input.setDescription(DEFAULT_INPUT);
    }

    private ChocoSimppGssSink(UUID id, ChocoSimppGssSink copyFromNode) {
        super(id, copyFromNode);
        input = copyFromNode.getInput().copyOf();
    }
    
    public String getUsername() {
        return getParameter(USER_NAME_PARAMETER_ID).getValueAsString();
    }

    public String getPassword() {
        return getParameter(PASSWORD_PARAMETER_ID).getValueAsString();
    }

    public String getEmail() {
        return getParameter(EMAIL_PARAMETER_ID).getValueAsString();
    }

    public String getWorkUrl() {
        return getParameter(WORK_SHEET_PARAMETER_ID).getValueAsString();
    }

    public String getDataUrl() {
        return getParameter(DATA_SHEET_PARAMETER_ID).getValueAsString();
    }

    public String getPlanUrl() {
        return getParameter(PLAN_SHEET_PARAMETER_ID).getValueAsString();
    }

    public String getResourceUrl() {
        return getParameter(RESOURCE_SHEET_PARAMETER_ID).getValueAsString();
    }
    
    public String getOptiMode() {
        return getParameter(OPTI_MODE_PARAMETER_ID).getValueAsString();
    }

    private ChocoSimppGssSink(ChocoSimppGssSink copyFromNode) {
        super(copyFromNode);
        this.input = copyFromNode.input.copyOf();
    }  
    
    public String getUserFieldName() {
        return getParameter(USER_FIELD_NAME_PARAMETER_ID).getValueAsString();
    }

    public String getPasswordFieldName() {
        return getParameter(PASSWORD_FIELD_NAME_PARAMETER_ID).getValueAsString();
    }

    public String getDataName() {
        return getParameter(DATA_NAME_PARAMETER_ID).getValueAsString();
    }

    public String getPlanName() {
        return getParameter(PLAN_NAME_PARAMETER_ID).getValueAsString();
    }

    public String getResourceName() {
        return getParameter(RESOURCE_NAME_PARAMETER_ID).getValueAsString();
    }
    
    public String getAttributeNames() {
        return getParameter(ATTRIBUTE_NAMES_PARAMETER_ID).getValueAsString();
    }    
    
    public int getLowValue() {
        return getParameter(LOW_VALUE_PARAMETER_ID).getValueAsInteger();
    }
    
    public int getLowCost() {
        return getParameter(LOW_COST_PARAMETER_ID).getValueAsInteger();
    }
    
    public int getHighValue() {
        return getParameter(HIGH_VALUE_PARAMETER_ID).getValueAsInteger();
    }
    
    public int getHighCost() {
        return getParameter(HIGH_COST_PARAMETER_ID).getValueAsInteger();
    }

    public int getPlanType() {
        return getParameter(PLAN_TYPE_PARAMETER_ID).getValueAsInteger();
    }

    public int getPrecisionCoeff() {
        return getParameter(PRECISION_PARAMETER_ID).getValueAsInteger();
    }

    public int getTimeOut() {
        return getParameter(TIME_OUT_PARAMETER_ID).getValueAsInteger();
    }
    
    public String getDomainOption() {
        return getParameter(VARIABLE_OPTION_PARAMETER_ID).getValueAsString();
    }
    
    public Input<Event> getInput() {
        return input;
    }

    @Override
    public List<Input<Event>> getInputs() {
        return ImmutableList.of(input);
    }

    @Override
    public boolean isConnectedTo(Source source) {
        return input.isConnectedTo(source);
    }

    @Override
    public void disconnect(Source source) {
        if (input.isConnectedTo(source)) {
            input.clearSource();
        }
    }

    @Override
    public ChocoSimppGssSink newInstance() {
        return new ChocoSimppGssSink(UUID.randomUUID(), this);
    }

    @Override
    public ChocoSimppGssSink copyOf() {
        return new ChocoSimppGssSink(this);
    }

    public static ChocoSimppGssSink newTemplate() {
        UUID sinkId = UUID.randomUUID();

        ChocoSimppGssSink simppSink = new ChocoSimppGssSink(sinkId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        
        simppSink.addParameter(Parameter.stringParameterWithIdAndName(USER_NAME_PARAMETER_ID, "User name:"));
        simppSink.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, "Password:")
                .defaultValue("isasdemo"));
        simppSink.addParameter(Parameter.stringParameterWithIdAndName(EMAIL_PARAMETER_ID, "Email")
                .defaultValue("demo@lisa-park.com"));

        simppSink.addParameter(Parameter.stringParameterWithIdAndName(WORK_SHEET_PARAMETER_ID,
                "Spreadsheet name:")
                .defaultValue("ProductionDemo")
                .required(true));
        simppSink.addParameter(Parameter.stringParameterWithIdAndName(DATA_SHEET_PARAMETER_ID,
                "Data sheet:")
                .defaultValue("data")
                .required(true));
        simppSink.addParameter(Parameter.stringParameterWithIdAndName(PLAN_SHEET_PARAMETER_ID,
                "Plan sheet:")
                .defaultValue("plan")
                .required(true));
        simppSink.addParameter(Parameter.stringParameterWithIdAndName(RESOURCE_SHEET_PARAMETER_ID,
                "Resource sheet:")
                .defaultValue("resources")
                .required(true));

        simppSink.addParameter(Parameter.stringParameterWithIdAndName(OPTI_MODE_PARAMETER_ID,
                "Optimization mode:").defaultValue("max")
                .description("Processor supports two optimization modes: minimum cost -"
                + " type MIN to choose it;"
                + " an maximum resource utilization -"
                + " type MAX, if you want use this mode.")
                .required(true));
        simppSink.addParameter(Parameter.stringParameterWithIdAndName(DATA_NAME_PARAMETER_ID,
                "Data field name:").defaultValue(DATA_NAME).required(true));
        simppSink.addParameter(Parameter.stringParameterWithIdAndName(PLAN_NAME_PARAMETER_ID,
                "Plan field name:").defaultValue(PLAN_NAME).required(true));
        simppSink.addParameter(Parameter.stringParameterWithIdAndName(RESOURCE_NAME_PARAMETER_ID,
                "Resource field name:").defaultValue(RESOURCE_NAME).required(true));
        simppSink.addParameter(Parameter.stringParameterWithIdAndName(ATTRIBUTE_NAMES_PARAMETER_ID,
                "Attribute names:").defaultValue(ATTRIBUTE_NAMES).required(false));
        simppSink.addParameter(Parameter.integerParameterWithIdAndName(LOW_VALUE_PARAMETER_ID,
                "Production low bound:").defaultValue(LOW_VALUE).required(false));
        simppSink.addParameter(Parameter.integerParameterWithIdAndName(HIGH_VALUE_PARAMETER_ID,
                "Production high bound:").defaultValue(HIGH_VALUE).required(false));
        simppSink.addParameter(Parameter.integerParameterWithIdAndName(HIGH_COST_PARAMETER_ID,
                "Production cost high bound:").defaultValue(HIGH_COST).required(false));
        simppSink.addParameter(Parameter.integerParameterWithIdAndName(LOW_COST_PARAMETER_ID,
                "Production cost low bound:").defaultValue(LOW_COST).required(false));
        
        simppSink.addParameter(Parameter.integerParameterWithIdAndName(PLAN_TYPE_PARAMETER_ID,
                "Plan strategy:").defaultValue(PLAN_TYPE_CAUTIOUS)
                .description("Processor supports three optimization strategy (1, 2, 3). Agressive"
                + " - Maximum production (1);"
                + " Balanced"
                + " - calculates production close to the plan (2);"
                + " Cautious"
                + " - calculates production, that does not exceed plan values(3).")
                .required(true));
        simppSink.addParameter(Parameter.integerParameterWithIdAndName(PRECISION_PARAMETER_ID,
                "Precision:").defaultValue(PRECISIONT_DEFAULT)
//                .description("Алгоритм оптимизации использует целые числа. Для перевода чисел с десятичными"
//                + " знаками необходимо использовать поправочный коэффициент кратный 10 в сответствующей степени."
//                + " Так, чтобы иметь два десятичных знака нужно использовать 100 как поправочный коэффициент; три - 1000 и т.д."
//                + " Использование поправочного коэффициента требует соответствующих измений для МАХ целевой функции.")
                .required(false));
        simppSink.addParameter(Parameter.integerParameterWithIdAndName(TIME_OUT_PARAMETER_ID,
                "Time out:").defaultValue(TIME_OUT_DEFAULT)
//                .description("Указывает максимальное время исполнения программы оптимизации."
//                + " Система выводит последнее найденное решение."
//                + " Слишком короткое время исполнения может быть недостаточным для получения даже единственного решения.")
                .required(true));
        
        simppSink.addParameter(Parameter.stringParameterWithIdAndName(VARIABLE_OPTION_PARAMETER_ID,
                "Variable options:").defaultValue(VARIABLE_OPTION).required(false));
        

        return simppSink;
    }

    @Override
    public CompiledExternalSink compile() {
        return new CompiledSimppSink(copyOf());
    }

    static class CompiledSimppSink extends CompiledExternalSink {
        
        private final ChocoSimppGssSink simppSink;
 
        protected CompiledSimppSink(ChocoSimppGssSink simppSink) {
            super(simppSink);            
            this.simppSink = simppSink;            
        }

        @Override
        public synchronized void processEvent(SinkContext ctx, Map<Integer, Event> eventsByInputId) {
            
            Event event = eventsByInputId.get(1);
             
            logger.log(Level.INFO, eventsByInputId.toString(), eventsByInputId);
            
            String optiMode = simppSink.getOptiMode();
            String dict     = simppSink.getAttributeNames();
            int precision   = simppSink.getPrecisionCoeff();
            
            if (event == null) {
                ctx.getStandardOut().println("Not all input data is presented.");
                return;
            }
            // Collect input data for each xml file
            String jsonData = (String) event.getData().get(simppSink.getDataName());
            String jsonPlan = (String) event.getData().get(simppSink.getPlanName());
            String jsonRes  = (String) event.getData().get(simppSink.getResourceName());

//            ctx.getStandardOut().println(jsonData);
//            ctx.getStandardOut().println(jsonPlan);
//            ctx.getStandardOut().println(jsonRes);

            //==========================================================
            // Convert string xml files to java collections  

            List<OptiBean> data = ChocoOptiUtils.getJsonStringAsOptiBeanList(jsonData, precision, dict);
            logger.log(Level.INFO, "data = {0}", data);

            Map<String, OptiBean> plan = ChocoOptiUtils.getOptiBeanMapFromJsonString(jsonPlan, precision, dict, OptiBean.KEY_PROD);
            logger.log(Level.INFO, "planDataMap = {0}", plan);

            Map<String, OptiBean> resource = ChocoOptiUtils.getOptiBeanMapFromJsonString(jsonRes, precision, dict, OptiBean.KEY_RESOURCE);
            logger.log(Level.INFO, "resourceDataMap = {0}", resource);

            Map<String, Map<String, Object>> objectMap = ChocoOptiUtils.getJsonStringAsMap(jsonData, precision, dict);
            logger.log(Level.INFO, "objectMap = {0}", objectMap);

            // Settting source data for the Choco model
            //======================================================================
            ChocoOptiModel optiModel = new ChocoOptiModel(data, plan, resource);

            // Set planning type that will define product plan constraints
            //      PLAN_TYPE_AGGRESSIVE - sets condition greater or equal
            //      chocoModel.addConstraint(Choco.geq(expPlanVars[j], intVar[i]));
            //      PLAN_TYPE_BALANCED - sets condition equal
            //      chocoModel.addConstraint(Choco.eq(expPlanVars[j], intVar[i]));
            //      PLAN_TYPE_CAUTIOUS - sets condition less or equal
            //      chocoModel.addConstraint(Choco.leq(expPlanVars[j], intVar[i]));
            optiModel.setPlanType(simppSink.getPlanType());
            optiModel.setDomainOption(simppSink.getDomainOption());
            optiModel.setProdAbsLow(simppSink.getLowValue());

            // Creating Variable Expressions for constraints
            //======================================================================
            IntegerExpressionVariable dataVarExp = optiModel.createDataTotalCostIntExpVariable();
            logger.log(Level.INFO, "dataVarExp = {0}", dataVarExp);

            IntegerExpressionVariable resourceVarExp = optiModel.createResourceTotalIntExtVariable();
            logger.log(Level.INFO, "rVarExp = {0}", resourceVarExp);

            int highcost    = simppSink.getHighCost() > 0 ? simppSink.getHighCost() : HIGH_COST;
            int highvalue   = simppSink.getHighValue() > 0 ? simppSink.getHighValue() : HIGH_VALUE;
            int lowcost     = simppSink.getLowCost() > 0 ? simppSink.getLowCost() : LOW_COST;
            int lowvalue    = simppSink.getLowValue() > 0 ? simppSink.getLowValue() : LOW_VALUE;

            // Add constraints to the model
            //======================================================================
            Model chocoModel = new CPModel();

            IntegerVariable goal;
            if ("MIN".equalsIgnoreCase(optiMode)) {
                goal = Choco.makeIntVar(ChocoOptiUtils.TOTAL_COST, lowcost, highcost, simppSink.getDomainOption());
            } else {
                goal = Choco.makeIntVar(ChocoOptiUtils.TOTAL_RESOURCES, lowvalue, highvalue, simppSink.getDomainOption());
            }

            chocoModel = optiModel.addDataUnitVariables(chocoModel);
            chocoModel = optiModel.addPlanVariables(chocoModel);
            chocoModel = optiModel.addResourceVariables(chocoModel);
            chocoModel = optiModel.addPlanConstraints(chocoModel);
            chocoModel = optiModel.addResourceConstraints(chocoModel);
            chocoModel = optiModel.addFixedProdConstraints(chocoModel);

            if ("MIN".equalsIgnoreCase(optiMode)) {
                chocoModel.addConstraint(Choco.eq(dataVarExp, goal));
            } else {
                chocoModel.addConstraint(Choco.eq(resourceVarExp, goal));
            }

            // Perform Simulation
            //======================================================================
            Solver solver = new CPSolver();
            solver.read(chocoModel);

            try {
                solver.propagate();
            } catch (ContradictionException e) {
                ctx.getStandardOut().println("ContradictionException: " + e);
            }

            solver.setValIntIterator(new DecreasingDomain());
            logger.log(Level.INFO, "solver.setValIntIterator:");

            solver.setTimeLimit(simppSink.getTimeOut());

            ChocoLogging.toVerbose();

            if ("MIN".equalsIgnoreCase(optiMode)) {
                solver.minimize(solver.getVar(goal), false);
                logger.log(Level.INFO, "Solutions MIN: {0}", solver.getNbSolutions());
            } else {
                solver.maximize(solver.getVar(goal), false);
                logger.log(Level.INFO, "Solutions MAX: {0}", solver.getNbSolutions());
            }
            
            //==========================================================
            OptiBean optiBean = OptiBean.newInstance(null, precision, dict);

            Map<String, IntegerVariable> intVarMap = optiModel.intVarMap(optiModel.getIntVarDataUnitArray());

            List<Map<String, Object>> objectList = optiModel.updateData(objectMap, intVarMap, optiBean.PROD_VALUE(), solver);

            ctx.getStandardOut().println(objectList);
            
            // Save results to the Google Stread Sheet
            if(objectList.size() > 0){
                sendToGss(objectList, null,
                        simppSink.getEmail(),
                        simppSink.getPassword(),
                        simppSink.getWorkUrl(),
                        simppSink.getDataUrl());
            }

            // CSP execution information
            //==========================================================
            logger.log(Level.INFO, "Solutions: {0}", solver.getNbSolutions());
            logger.log(Level.INFO, "Goal value: {0}", solver.getOptimumValue());
            logger.log(Level.INFO, "Constraints value: {0}", solver.getModel().constraintsToString());
            logger.log(Level.INFO, "Solution value: {0}", solver.getModel().solutionToString());
            logger.log(Level.INFO, "getIntDecisionVars: {0}", solver.getIntDecisionVars());

            IntDomainVar[] domainVars = solver.getIntDecisionVars();
            for (int i = 0; i < domainVars.length; i++) {
                logger.log(Level.INFO, "getIntDecisionVars: {0}", domainVars[i].getName()
                        + ": " + domainVars[i].getSup());
            }

            ChocoLogging.flushLogs();

            solver.getEnvironment().clear();
        }

        private void sendToGss(List<Map<String, Object>> objectList,
                String formulas,
                String userEmail,
                String password,
                String workUrl,
                String dataUrl) {
            GssListUtils gssList = new GssListUtils(simppSink.getId().toString().replace("-", ""),
                    workUrl,
                    dataUrl,
                    userEmail,
                    password);
            try {
                ListFeed listFeed = gssList.loadSheet();

                if (listFeed.getEntries().isEmpty()) {
                    for (Map<String, Object> newEntry : objectList) {
                        ListEntry entry = gssList.addNewEntryValues(newEntry, null);
                        if (formulas != null && !formulas.isEmpty()) {
                            gssList.updateEntryFormulas(entry, newEntry, formulas);
                        }
                        gssList.getService().insert(gssList.getListFeedUrl(), entry);
                    }
                } else {
                    int i = 0;
                    for (Map<String, Object> newEntry : objectList) {
                        ListEntry entry = gssList.updateEntryValues(listFeed.getEntries().get(i), newEntry, null);
                        if (formulas != null && !formulas.isEmpty()) {
                            gssList.updateEntryFormulas(entry, newEntry, formulas);
                        }
                        entry.update();
                        i++;
                    }
                }

            } catch (ServiceException ex) {
                Exceptions.printStackTrace(ex);
            } catch (IOException ex) {
                Exceptions.printStackTrace(ex);
            }
        }
    }   
}
