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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import javax.xml.parsers.ParserConfigurationException;
import org.lisapark.octopus.core.AbstractNode;
import org.lisapark.octopus.core.Input;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.SinkContext;
import org.lisapark.octopus.core.sink.external.CompiledExternalSink;
import org.lisapark.octopus.core.sink.external.ExternalSink;
import org.lisapark.octopus.core.source.Source;
import org.lisapark.octopus.util.optimodel.OptiBean;
import org.lisapark.octopus.util.optimodel.ChocoOptiModel;
import org.lisapark.octopus.util.optimodel.ChocoOptiUtils;
import org.openide.util.Exceptions;
import org.xml.sax.SAXException;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
@Persistable
public class ChocoSimppXmlWebDavSink extends AbstractNode implements ExternalSink {
    
    private final static java.util.logging.Logger logger 
            = java.util.logging.Logger.getLogger(ChocoSimppXmlWebDavSink.class.getName());

    private static final String DEFAULT_NAME = "СИМПП: выполнить и сохранить на Web DAV";
    private static final String DEFAULT_DESCRIPTION = "Принимает исходные данные как строковые переменные,"
            + " выполняет симуляционное моделирование для планирования производства"
            + " и сохраняет результат в виде XML файла на Web Dav Server.";
    private static final int USER_NAME_PARAMETER_ID             = 1;
    private static final int PASSWORD_PARAMETER_ID              = 2;
    private static final int DATA_URL_PARAMETER_ID              = 3;
    private static final int PLAN_URL_PARAMETER_ID              = 4;
    private static final int RESOURCE_URL_PARAMETER_ID          = 5;
    private static final int DATA_NAME_PARAMETER_ID             = 6;
    private static final int PLAN_NAME_PARAMETER_ID             = 7;
    private static final int RESOURCE_NAME_PARAMETER_ID         = 8;
    private static final int ATTRIBUTE_NAMES_PARAMETER_ID       = 9;
    
    private static final int LOW_VALUE_PARAMETER_ID             = 10;
    private static final int HIGH_VALUE_PARAMETER_ID            = 11;
    
    private static final int LOW_COST_PARAMETER_ID              = 19;
    
    private static final int HIGH_COST_PARAMETER_ID             = 12;
    private static final int OPTI_MODE_PARAMETER_ID             = 13;
    private static final int PLAN_TYPE_PARAMETER_ID             = 14;
    
    private static final int USER_FIELD_NAME_PARAMETER_ID       = 15;
    private static final int PASSWORD_FIELD_NAME_PARAMETER_ID   = 16;
    
    private static final int PRECISION_PARAMETER_ID             = 17;
    
    private static final int TIME_OUT_PARAMETER_ID              = 18;
    
    private static final int VARIABLE_OPTION_PARAMETER_ID       = 20;
    
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
            = "PROD=PRODUCT_ID,"
            + "PROCESS_STEP=TECHNOLOGY_STEP_NUMBER,"
            + "PROCESSOR=MACHINE_ID,"
            + "PROCESS_COST=COST_PER_UNIT,"            
            + "UNIT_VALUE=UNIT_VALUE,"
            + "PROD_VALUE=PROD_VALUE,"
            + "LOW_BOUND=LOWBOUND,"
            + "UPPER_BOUND=UPPERBOUND,"
            + "PLAN_VALUE=PLAN_VALUE,"
            + "RESOURCE=MACHINE_ID,"
            + "RESOURCE_VALUE=RESOURCE_VALUE,"
            + "FIXED=FIXED"
            ;
    
    public static final int LOW_VALUE           = 0;
    public static final int HIGH_VALUE          = 10000000;
    public static final int LOW_COST            = 0;
    public static final int HIGH_COST           = 10000000;
    
    public static final int PRECISIONT_DEFAULT  = 1;
    public static final int TIME_OUT_DEFAULT    = 30000;
    
    private static final String DEFAULT_INPUT   = "Входные данные";    
        
    private Input<Event> input;

    private ChocoSimppXmlWebDavSink(UUID id, String name, String description) {
        super(id, name, description);
        input = Input.eventInputWithId(1);
        input.setName(DEFAULT_INPUT);
        input.setDescription(DEFAULT_INPUT);
    }

    private ChocoSimppXmlWebDavSink(UUID id, ChocoSimppXmlWebDavSink copyFromNode) {
        super(id, copyFromNode);
        input = copyFromNode.getInput().copyOf();
    }
    
    public String getUsername() {
        return getParameter(USER_NAME_PARAMETER_ID).getValueAsString();
    }

    public String getPassword() {
        return getParameter(PASSWORD_PARAMETER_ID).getValueAsString();
    }

    public String getDataUrl() {
        return getParameter(DATA_URL_PARAMETER_ID).getValueAsString();
    }

    public String getPlanUrl() {
        return getParameter(PLAN_URL_PARAMETER_ID).getValueAsString();
    }

    public String getResourceUrl() {
        return getParameter(RESOURCE_URL_PARAMETER_ID).getValueAsString();
    }
    
    public String getOptiMode() {
        return getParameter(OPTI_MODE_PARAMETER_ID).getValueAsString();
    }

    private ChocoSimppXmlWebDavSink(ChocoSimppXmlWebDavSink copyFromNode) {
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
    public ChocoSimppXmlWebDavSink newInstance() {
        return new ChocoSimppXmlWebDavSink(UUID.randomUUID(), this);
    }

    @Override
    public ChocoSimppXmlWebDavSink copyOf() {
        return new ChocoSimppXmlWebDavSink(this);
    }

    public static ChocoSimppXmlWebDavSink newTemplate() {
        UUID sinkId = UUID.randomUUID();

        ChocoSimppXmlWebDavSink simppSink = new ChocoSimppXmlWebDavSink(sinkId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        
        simppSink.addParameter(Parameter.stringParameterWithIdAndName(USER_NAME_PARAMETER_ID, "User name"));
        simppSink.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, "Password"));

        simppSink.addParameter(Parameter.stringParameterWithIdAndName(DATA_URL_PARAMETER_ID,
                "URL для выходного файла Данных")
                .defaultValue("http://173.72.110.131:8080/WebDavServer/iPlast/results/Data.xml")
                .required(true));
        simppSink.addParameter(Parameter.stringParameterWithIdAndName(PLAN_URL_PARAMETER_ID,
                "URL для выходного файла Плана")
                .defaultValue("http://173.72.110.131:8080/WebDavServer/iPlast/results/Plan.xml")
                .required(true));
        simppSink.addParameter(Parameter.stringParameterWithIdAndName(RESOURCE_URL_PARAMETER_ID,
                "URL для выходногофайла Ресурсов")
                .defaultValue("http://173.72.110.131:8080/WebDavServer/iPlast/results/Resources.xml")
                .required(true));

        simppSink.addParameter(Parameter.stringParameterWithIdAndName(OPTI_MODE_PARAMETER_ID,
                "Режим оптимизации").defaultValue("min")
                .description("Процессор поддерживает два режима оптимизации: минимизация себестоимости -"
                + " для выбора этого режима используйте MIN;"
                + " и максимизация использования оборудования -"
                + " для выбора этого режима напечатайте MAX.")
                .required(true));
        simppSink.addParameter(Parameter.stringParameterWithIdAndName(DATA_NAME_PARAMETER_ID,
                "Имя поля для Данных").defaultValue(DATA_NAME).required(true));
        simppSink.addParameter(Parameter.stringParameterWithIdAndName(PLAN_NAME_PARAMETER_ID,
                "Имя поля для Плана").defaultValue(PLAN_NAME).required(true));
        simppSink.addParameter(Parameter.stringParameterWithIdAndName(RESOURCE_NAME_PARAMETER_ID,
                "Имя поля для Ресурсов").defaultValue(RESOURCE_NAME).required(true));
        simppSink.addParameter(Parameter.stringParameterWithIdAndName(ATTRIBUTE_NAMES_PARAMETER_ID,
                "Имена атрибутов").defaultValue(ATTRIBUTE_NAMES).required(false));
        simppSink.addParameter(Parameter.integerParameterWithIdAndName(LOW_VALUE_PARAMETER_ID,
                "Нижняя граница объема производства").defaultValue(LOW_VALUE).required(false));
        simppSink.addParameter(Parameter.integerParameterWithIdAndName(HIGH_VALUE_PARAMETER_ID,
                "Верхняя граница объема производства").defaultValue(HIGH_VALUE).required(false));
        simppSink.addParameter(Parameter.integerParameterWithIdAndName(HIGH_COST_PARAMETER_ID,
                "Верхняя граница стоимости производства").defaultValue(HIGH_COST).required(false));
        simppSink.addParameter(Parameter.integerParameterWithIdAndName(LOW_COST_PARAMETER_ID,
                "Нижняя граница стоимости производства").defaultValue(LOW_COST).required(false));
        
        simppSink.addParameter(Parameter.integerParameterWithIdAndName(PLAN_TYPE_PARAMETER_ID,
                "Стратегия плана").defaultValue(PLAN_TYPE_BALANCED)
                .description("Прoцессор поддерживает три стратегии (1, 2, 3) для оптимизации плана. Агрессивная"
                + " - максимизирует плановые показатели (1);"
                + " Сбалансированная"
                + " - рассчитывает вариант плана со значениями близкими к заданным (2);"
                + " Осторожная"
                + " - рассчитывает план, который не превосходит заданных значений (3).")
                .required(true));
        simppSink.addParameter(Parameter.integerParameterWithIdAndName(PRECISION_PARAMETER_ID,
                "Точность вычислений").defaultValue(PRECISIONT_DEFAULT)
                .description("Алгоритм оптимизации использует целые числа. Для перевода чисел с десятичными"
                + " знаками необходимо использовать поправочный коэффициент кратный 10 в сответствующей степени."
                + " Так, чтобы иметь два десятичных знака нужно использовать 100 как поправочный коэффициент; три - 1000 и т.д."
                + " Использование поправочного коэффициента требует соответствующих измений для МАХ целевой функциию.")
                .required(false));
        simppSink.addParameter(Parameter.integerParameterWithIdAndName(TIME_OUT_PARAMETER_ID,
                "МАХ время рассчета").defaultValue(TIME_OUT_DEFAULT)
                .description("Указывает максимальное время исполнения программы оптимизации."
                + " Система выводит последнее найденное решение."
                + " Слишком короткое время исполнения может быть недостаточным для получения даже единственного решения.")
                .required(true));
        
        simppSink.addParameter(Parameter.stringParameterWithIdAndName(VARIABLE_OPTION_PARAMETER_ID,
                "Опции для переменных").defaultValue(VARIABLE_OPTION).required(false));
        

        return simppSink;
    }

    @Override
    public CompiledExternalSink compile() {
        return new CompiledSimppSink(copyOf());
    }

    static class CompiledSimppSink extends CompiledExternalSink {
        
        private final ChocoSimppXmlWebDavSink simppSink;
        
        protected CompiledSimppSink(ChocoSimppXmlWebDavSink simppSink) {
            super(simppSink);            
            this.simppSink = simppSink;            
        }

        @Override
        public synchronized void processEvent(SinkContext ctx, Map<Integer, Event> eventsByInputId) {
            
            Event event = eventsByInputId.get(1);
             
            logger.log(Level.INFO, eventsByInputId.toString(), eventsByInputId);
            
            String optiMode = simppSink.getOptiMode();
            String dict = simppSink.getAttributeNames();
            int precision = simppSink.getPrecisionCoeff();
            
            if (event != null) {
                
                try {
                    // Collect input data for each xml file
                    String xmlData = (String) event.getData().get(simppSink.getDataName());
                    String xmlPlan = (String) event.getData().get(simppSink.getPlanName());
                    String xmlRes = (String) event.getData().get(simppSink.getResourceName());

                    ctx.getStandardOut().println(xmlData);
                    ctx.getStandardOut().println(xmlPlan);
                    ctx.getStandardOut().println(xmlRes);

                    //==========================================================
                    // Convert string xml files to java collections  
                    
                    List<OptiBean> data = ChocoOptiUtils.getXmlStringAsOptiBeanList(xmlData, precision, dict);
//                    logger.log(Level.INFO, "data = {0}", data);

                    Map<String, OptiBean> plan = ChocoOptiUtils.getOptiBeanPlanMapFromXmlString(xmlPlan, precision, dict);
//                    logger.log(Level.INFO, "planDataMap = {0}", plan);

                    Map<String, OptiBean> resource = ChocoOptiUtils.getOptiBeanResourceMapFromXmlString(xmlRes, precision, dict);
//                    logger.log(Level.INFO, "resourceDataMap = {0}", resource);

//                    List<Map<String, Object>> objectList = ChocoOptiUtils.getXmlStringAsList(xmlData, precision, dict);
//                    logger.log(Level.INFO, "objectList = {0}", objectList);

                    Map<String, Map<String, Object>> objectMap = ChocoOptiUtils.getXmlStringAsMap(xmlData, precision, dict);
//                    logger.log(Level.INFO, "objectMap = {0}", objectMap);

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

                    int highcost = simppSink.getHighCost() > 0 ? simppSink.getHighCost() : HIGH_COST;                    
                    int highvalue = simppSink.getHighValue() > 0 ? simppSink.getHighValue() : HIGH_VALUE;                    
                    int lowcost = simppSink.getLowCost() > 0 ? simppSink.getLowCost() : LOW_COST;                    
                    int lowvalue = simppSink.getLowValue() > 0 ? simppSink.getLowValue() : LOW_VALUE;
                    
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
                    } else {//
                        solver.maximize(solver.getVar(goal), false);
                        logger.log(Level.INFO, "Solutions MAX: {0}", solver.getNbSolutions());
                    }
                    //==========================================================
                    OptiBean optiBean = OptiBean.newInstance(null, precision, dict);

                    Map<String, IntegerVariable> intVarMap = optiModel.intVarMap(optiModel.getIntVarDataUnitArray());

                    List<Map<String, Object>> objectList = optiModel.updateData(objectMap, intVarMap, optiBean.PROD_VALUE(), solver);

                    String outputString = ChocoOptiUtils.formatOutput(objectList);

                    ChocoOptiUtils.saveStringAsFile(outputString, simppSink.getDataUrl());

                    ctx.getStandardOut().println(outputString);

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

                } catch (ParserConfigurationException ex) {
                    Exceptions.printStackTrace(ex);
                } catch (SAXException ex) {
                    Exceptions.printStackTrace(ex);
                } catch (IOException ex) {
                    Exceptions.printStackTrace(ex);
                }
            } else {
                ctx.getStandardOut().println("Not all input data is presented.");
            }
        }
    }   
        
}
