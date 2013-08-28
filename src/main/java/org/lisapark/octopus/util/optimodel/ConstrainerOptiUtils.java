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
package org.lisapark.octopus.util.optimodel;

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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.googlecode.sardine.DavResource;
import com.googlecode.sardine.Sardine;
import com.googlecode.sardine.SardineFactory;
import com.googlecode.sardine.impl.SardineException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.commons.io.IOUtils;
import org.lisapark.octopus.util.xml.XmlUtils;
import org.openide.util.Exceptions;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class ConstrainerOptiUtils {

    private final static java.util.logging.Logger logger = java.util.logging.Logger.getLogger(XmlUtils.class.getName());
    public static final String TOTAL_COST = "TOTAL_COST";
    public static final String TOTAL_RESOURCES = "TOTAL_RESOURCEs";
    public static final String FAILURE = "FAILURE";
    public static final String SUCCESS = "SUCCESS";
    public static final String FALSE = "FALSE";
    public static final String TRUE = "TRUE";
    private static int HIGH_COST = 10000000;
    private static int HIGH_RESOURCES = 100000;

    public static void main(String[] args)
            throws ParserConfigurationException, SAXException, IOException {

        // Set measure Coefficient
        int precision = 1;

        String dictionary =
                OptiBean.KEY_PROD + "=PRODUCT_ID,"
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
                + "FIXED=FIXED";

        String dataFile = "http://173.72.110.131:8080/WebDavServer/iPlast/Data.xml";
        String planFile = "http://173.72.110.131:8080/WebDavServer/iPlast/Plan.xml";
        String recFile = "http://173.72.110.131:8080/WebDavServer/iPlast/Resources.xml";

        // Get all xml files
        Sardine sardine = SardineFactory.begin("", "");

        InputStream isData = sardine.get(dataFile);
        String xmlData = IOUtils.toString(isData);

        InputStream isPlan = sardine.get(planFile);
        String xmlPlan = IOUtils.toString(isPlan);

        InputStream isRec = sardine.get(recFile);
        String xmlMach = IOUtils.toString(isRec);

        // Conver string xml files to java collections        
        List<OptiBean> data = getXmlStringAsOptiBeanList(xmlData, precision, dictionary);
        logger.log(Level.INFO, "data = {0}", data);

//        OptiBean optiBeanPlan = OptiBean.newInstance(null, dictionary);
        Map<String, OptiBean> plan = getOptiBeanPlanMapFromXmlString(xmlPlan, precision, dictionary);
        logger.log(Level.INFO, "planDataMap = {0}", plan);

//        OptiBean optiBeanMach = OptiBean.newInstance(null, dictionary);
        Map<String, OptiBean> machine = getOptiBeanResourceMapFromXmlString(xmlMach, precision, dictionary);
        logger.log(Level.INFO, "machineDataMap = {0}", machine);

        List<Map<String, Object>> objectList = getXmlStringAsList(xmlData, precision, dictionary);
        logger.log(Level.INFO, "objectList = {0}", objectList);

        Map<String, Map<String, Object>> objectMap = getXmlStringAsMap(xmlData, precision, dictionary);
        logger.log(Level.INFO, "objectMap = {0}", objectMap);

        // Settting source data for the Choco model
        //======================================================================

        ChocoOptiModel optiModel = new ChocoOptiModel(data, plan, machine);

        optiModel.setPlanType(ChocoOptiModel.PLAN_TYPE_CAUTIOUS);
//        optiModel.setPlanType(OptiModel.PLAN_TYPE_BALANCED);

        logger.log(Level.INFO, "getProductConstantList = {0}", optiModel.getPlanConstantList());
        logger.log(Level.INFO, "getProductConstantMap = {0}", optiModel.getPlanConstantMap());
        logger.log(Level.INFO, "getResourceConstantList = {0}", optiModel.getResourceConstantList());
        logger.log(Level.INFO, "getResourceConstantMap = {0}", optiModel.getResourceConstantMap());

//        IntegerVariable[] dataUnitIntVar = optiModel.createIntVarDataUnitArray();
//        logger.log(Level.INFO,  "dataIntVar = {0}", dataUnitIntVar);
////        
////        IntegerVariable[] dataAbsIntVar = optiModel.createIntVarDataAbsArray();
////        logger.log(Level.INFO,  "dataIntVar = {0}", dataAbsIntVar);
//        
//        IntegerVariable[] planIntVar = optiModel.createIntVarPlanArray();
//        logger.log(Level.INFO,  "planIntVar = {0}", planIntVar);
//        
//        IntegerVariable[] machIntVar = optiModel.createIntVarResourceArray();
//        logger.log(Level.INFO,  "machIntVar = {0}", machIntVar);

        // Creating Variable Expressions for constraints
        //======================================================================
        //======================================================================

        IntegerExpressionVariable dataVarExp = optiModel.createDataTotalCostIntExpVariable();
        logger.log(Level.INFO, "dataVarExp = {0}", dataVarExp);

        IntegerExpressionVariable machVarExp = optiModel.createResourceTotalIntExtVariable();
        logger.log(Level.INFO, "dataVarExp = {0}", machVarExp);

//        IntegerVariable goalCost        = Choco.makeIntVar(TOTAL_COST, 1, HIGH_COST, Options.V_BOUND);
        IntegerVariable goalResources = Choco.makeIntVar(TOTAL_RESOURCES, 1000, HIGH_RESOURCES, Options.V_BOUND);

        // Add constraints to the model
        //======================================================================
        Model chocoModel = new CPModel();

//        chocoModel = optiModel.addDataAbsVariables(chocoModel);
        chocoModel = optiModel.addDataUnitVariables(chocoModel);
        chocoModel = optiModel.addPlanVariables(chocoModel);
        chocoModel = optiModel.addResourceVariables(chocoModel);
        chocoModel = optiModel.addPlanConstraints(chocoModel);
        chocoModel = optiModel.addResourceConstraints(chocoModel);
        chocoModel = optiModel.addFixedProdConstraints(chocoModel);

//        chocoModel.addConstraint(Choco.leq(dataVarExp, goalCost));
        chocoModel.addConstraint(Choco.leq(machVarExp, goalResources));

        // Perform Simulation
        //======================================================================

        Solver solver = new CPSolver();

        solver.read(chocoModel);

        try {
            solver.propagate();
        } catch (ContradictionException e) {
            e.printStackTrace();
        }

        solver.setValIntIterator(new DecreasingDomain());

        logger.log(Level.INFO, "solver.setValIntIterator:");

        solver.setTimeLimit(30000);
        ChocoLogging.toVerbose();
//                
//        solver.minimize(solver.getVar(goalCost), false);
//        logger.log(Level.INFO, "Solutions MIN: {0}", solver.getNbSolutions());

        solver.maximize(solver.getVar(goalResources), false);
////        solver.generateSearchStrategy();
//        
        logger.log(Level.INFO, "Solutions MAX: {0}", solver.getNbSolutions());

//        logger.log(Level.INFO, "solver.minimize:");

        //======================================================================

        OptiBean optiBean = OptiBean.newInstance(null, precision, dictionary);

        Map<String, IntegerVariable> intVarMap = optiModel.intVarMap(optiModel.getIntVarDataUnitArray());

        objectList = optiModel.updateData(objectMap, intVarMap, optiBean.PROD_VALUE(), solver);


        String outputString = formatOutput(objectList);

        System.out.println(outputString);

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

    /**
     *
     * @param xmlData
     * @param optiBean
     * @return
     * @throws ParserConfigurationException
     * @throws SAXException
     * @throws IOException
     */
    public static List<OptiBean> getXmlStringAsOptiBeanList(String xmlData, int measureCoeff, String dict)
            throws ParserConfigurationException, SAXException, IOException {

        String cleanString = clean(xmlData);

//        logger.log(Level.INFO, xmlString, xmlString);

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document xmlDoc = builder.parse(new InputSource(new StringReader(cleanString)));

        List<OptiBean> mapList = optiBeanListFromXmlObject(xmlDoc, measureCoeff, dict);

        return mapList;
    }

    /**
     *
     * @param xmlString
     * @param optiBean
     * @return
     * @throws ParserConfigurationException
     * @throws SAXException
     * @throws IOException
     */
    public static synchronized List<Map<String, Object>> getXmlStringAsList(String xmlString, int measureCoeff, String dict)
            throws ParserConfigurationException, SAXException, IOException {

        String cleanString = clean(xmlString);

//        logger.log(Level.INFO, xmlString, xmlString);

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document xmlDoc = builder.parse(new InputSource(new StringReader(cleanString)));

        List<Map<String, Object>> mapList = mapListFromXmlObject(xmlDoc, measureCoeff, dict);

        return mapList;
    }

    /**
     *
     * @param xmlString
     * @param optiBean
     * @return
     * @throws ParserConfigurationException
     * @throws SAXException
     * @throws IOException
     */
    public static Map<String, OptiBean> getOptiBeanPlanMapFromXmlString(String xmlString, int measureCoeff, String dict)
            throws ParserConfigurationException, SAXException, IOException {

        String cleanString = clean(xmlString);

//        logger.log(Level.INFO, xmlString, xmlString);

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document xmlDoc = builder.parse(new InputSource(new StringReader(cleanString)));

        Map<String, OptiBean> mapList = optiBeanPlanMapFromXmlObject(xmlDoc, measureCoeff, dict);

        return mapList;
    }

    /**
     *
     * @param xmlString
     * @param optiBean
     * @return
     * @throws ParserConfigurationException
     * @throws SAXException
     * @throws IOException
     */
    public static Map<String, OptiBean> getOptiBeanResourceMapFromXmlString(String xmlString, int measureCoeff, String dict)
            throws ParserConfigurationException, SAXException, IOException {

        String cleanString = clean(xmlString);

//        logger.log(Level.INFO, xmlString, xmlString);

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document xmlDoc = builder.parse(new InputSource(new StringReader(cleanString)));

        Map<String, OptiBean> mapList = optiBeanMachineMapFromXmlObject(xmlDoc, measureCoeff, dict);

        return mapList;
    }

    /**
     *
     * @param xmlString
     * @param optiBean
     * @return
     * @throws ParserConfigurationException
     * @throws SAXException
     * @throws IOException
     */
    public static synchronized Map<String, Map<String, Object>> getXmlStringAsMap(String xmlString, int measureCoeff, String dict)
            throws ParserConfigurationException, SAXException, IOException {

        String cleanString = clean(xmlString);

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document xmlDoc = builder.parse(new InputSource(new StringReader(cleanString)));

        Map<String, Map<String, Object>> mapList = mapMapFromXmlObject(xmlDoc, measureCoeff, dict);

        return mapList;
    }

    /**
     *
     * @param xmlDoc
     * @param optiBean
     * @return
     */
    private static synchronized List<OptiBean> optiBeanListFromXmlObject(Document xmlDoc, int measureCoeff, String dict) {
        Element rootElement = xmlDoc.getDocumentElement();
        List<OptiBean> mapList = Lists.newArrayList();
        NodeList itemList = rootElement.getChildNodes();
        for (int j = 0; j < itemList.getLength(); j++) {
            Node grid = itemList.item(j);
            NodeList gridList = grid.getChildNodes();
            if (gridList.getLength() > 0) {
                Node body = gridList.item(0);
                NodeList bodyList = body.getChildNodes();
                int bodyListLenght = bodyList.getLength();
                if (bodyListLenght > 0) {
                    for (int i = 0; i < bodyListLenght; i++) {
                        Node item = bodyList.item(i);
                        NamedNodeMap itemMap = item.getAttributes();

                        if (itemMap == null) {
                            System.out.println("List Node[" + i + "] item is null!!!");
                            continue;
                        }
                        Map<String, Object> data = getGridItemMap(itemMap, dict);
                        OptiBean optiBean = OptiBean.newInstance(data, measureCoeff, dict);

                        mapList.add(optiBean);
                    }
                }
            }
        }
        return mapList;
    }

    /**
     *
     * @param xmlDoc
     * @param optiBean
     * @return
     */
    public static synchronized Map<String, OptiBean> optiBeanPlanMapFromXmlObject(Document xmlDoc, int measureCoeff, String dict) {

        Element rootElement = xmlDoc.getDocumentElement();

        Map<String, OptiBean> map = Maps.newHashMap();

        NodeList itemList = rootElement.getChildNodes();
        for (int j = 0; j < itemList.getLength(); j++) {
            Node grid = itemList.item(j);
            NodeList gridList = grid.getChildNodes();
            if (gridList.getLength() > 0) {
                Node body = gridList.item(0);
                NodeList bodyList = body.getChildNodes();
                int bodyListLenght = bodyList.getLength();
                if (bodyListLenght > 0) {
                    for (int i = 0; i < bodyListLenght; i++) {
                        Node item = bodyList.item(i);
                        NamedNodeMap itemMap = item.getAttributes();

                        if (itemMap == null) {
                            System.out.println("List Node[" + i + "] item is null!!!");
                            continue;
                        }
                        Map<String, Object> data = getGridItemMap(itemMap, dict);
                        OptiBean optiBean = OptiBean.newInstance(data, measureCoeff, dict);
                        String name;
                        name = optiBean.getProd();
                        map.put(name, optiBean);
                    }
                }
            }
        }
        return map;
    }

    /**
     *
     * @param xmlDoc
     * @param optiBean
     * @return
     */
    private static synchronized Map<String, OptiBean> optiBeanMachineMapFromXmlObject(Document xmlDoc, int measureCoeff, String dict) {

        Element rootElement = xmlDoc.getDocumentElement();

        Map<String, OptiBean> map = Maps.newHashMap();

        NodeList itemList = rootElement.getChildNodes();
        for (int j = 0; j < itemList.getLength(); j++) {
            Node grid = itemList.item(j);
            NodeList gridList = grid.getChildNodes();
            if (gridList.getLength() > 0) {
                Node body = gridList.item(0);
                NodeList bodyList = body.getChildNodes();
                int bodyListLenght = bodyList.getLength();
                if (bodyListLenght > 0) {
                    for (int i = 0; i < bodyListLenght; i++) {
                        Node item = bodyList.item(i);
                        NamedNodeMap itemMap = item.getAttributes();

                        if (itemMap == null) {
                            System.out.println("List Node[" + i + "] item is null!!!");
                            continue;
                        }
                        Map<String, Object> data = getGridItemMap(itemMap, dict);
                        OptiBean optiBean = OptiBean.newInstance(data, measureCoeff, dict);
                        String name;
                        name = optiBean.getResource();
                        map.put(name, optiBean);
                    }
                }
            }
        }
        return map;
    }

    /**
     *
     * @param xmlDoc
     * @param optiBean
     * @return
     * @throws DOMException
     * @throws NumberFormatException
     */
    private static List<Map<String, Object>> mapListFromXmlObject(Document xmlDoc, int measureCoeff, String dict)
            throws DOMException, NumberFormatException {
        //            Document xmlDoc = XmlTreeGridUtils.parseXML(xmlString.trim());
        Element rootElement = xmlDoc.getDocumentElement();
        List<Map<String, Object>> mapList = Lists.newArrayList();
        NodeList itemList = rootElement.getChildNodes();
        for (int j = 0; j < itemList.getLength(); j++) {
            Node grid = itemList.item(j);
            NodeList gridList = grid.getChildNodes();
            if (gridList.getLength() > 0) {
                Node body = gridList.item(0);
                NodeList bodyList = body.getChildNodes();
                int bodyListLenght = bodyList.getLength();
                if (bodyListLenght > 0) {
                    for (int i = 0; i < bodyListLenght; i++) {
                        Node item = bodyList.item(i);
                        NamedNodeMap itemMap = item.getAttributes();

                        if (itemMap == null) {
                            System.out.println("List Node[" + i + "] item is null!!!");
                            continue;
                        }
                        Map<String, Object> dataItem = getGridItemMap(itemMap, dict);
                        mapList.add(dataItem);
                    }
                }
            }
        }
        return mapList;
    }

    /**
     *
     * @param xmlDoc
     * @param optiBean
     * @return
     * @throws DOMException
     * @throws NumberFormatException
     */
    private static Map<String, Map<String, Object>> mapMapFromXmlObject(Document xmlDoc, int measureCoeff, String dict)
            throws DOMException, NumberFormatException {
        //            Document xmlDoc = XmlTreeGridUtils.parseXML(xmlString.trim());
        Element rootElement = xmlDoc.getDocumentElement();
        Map<String, Map<String, Object>> mapList = Maps.newHashMap();
        NodeList itemList = rootElement.getChildNodes();
        for (int j = 0; j < itemList.getLength(); j++) {
            Node grid = itemList.item(j);
            NodeList gridList = grid.getChildNodes();
            if (gridList.getLength() > 0) {
                Node body = gridList.item(0);
                NodeList bodyList = body.getChildNodes();
                int bodyListLenght = bodyList.getLength();
                if (bodyListLenght > 0) {
                    for (int i = 0; i < bodyListLenght; i++) {
                        Node item = bodyList.item(i);
                        NamedNodeMap itemMap = item.getAttributes();

                        if (itemMap == null) {
                            System.out.println("List Node[" + i + "] item is null!!!");
                            continue;
                        }
                        Map<String, Object> dataItem = getGridItemMap(itemMap, dict);
                        OptiBean optiBean = OptiBean.newInstance(dataItem, measureCoeff, dict);
                        String name = getName(dataItem, optiBean);
                        mapList.put(name, dataItem);
                    }
                }
            }
        }
        return mapList;
    }

    /**
     *
     * @param itemMap
     * @return
     * @throws NumberFormatException
     * @throws DOMException
     */
    private static Map<String, Object> getGridItemMap(NamedNodeMap itemMap, String dict)
            throws NumberFormatException, DOMException {

        Map<String, Object> dataItem = Maps.newHashMap();
        OptiBean optiBean = OptiBean.newInstance(null, 1, dict);
        for (int k = 0; k < itemMap.getLength(); k++) {
            Node attr = itemMap.item(k);
            String attrName = attr.getNodeName();

            if (optiBean.FIXED().equalsIgnoreCase(attrName)
                    || optiBean.LOW_BOUND().equalsIgnoreCase(attrName)
                    || optiBean.UPPER_BOUND().equalsIgnoreCase(attrName)
                    || optiBean.PLAN_VALUE().equalsIgnoreCase(attrName)
                    || optiBean.PROCESS_COST().equalsIgnoreCase(attrName)
                    || optiBean.PROD_VALUE().equalsIgnoreCase(attrName)
                    || optiBean.RESOURCE_VALUE().equalsIgnoreCase(attrName)
                    || optiBean.PROCESS_STEP().equalsIgnoreCase(attrName)
                    || optiBean.UNIT_VALUE().equalsIgnoreCase(attrName)) {
                String attrString = attr.getTextContent();
                int value = attrString.isEmpty() ? 0 : Integer.parseInt(attrString);
                dataItem.put(attrName, value);
            } else {
                String attrString = attr.getTextContent();
                dataItem.put(attrName, attrString);
            }
        }
        return dataItem;
    }

    /**
     *
     * @param string
     * @return
     */
    public synchronized static String clean(String string) {
        StringBuilder cleanStr = new StringBuilder();

        Character lookFor = '<';

        for (Character ch : string.toCharArray()) {
            if (ch == lookFor) {
                lookFor = lookFor == '<' ? '>' : '<';
                cleanStr.append(ch);
            } else if (lookFor == '>') {
                cleanStr.append(ch);
            }
        }

        return cleanStr.toString();
    }

    public static String formatOutput(List<Map<String, Object>> objectList) {
        // If objectList is empty return key word FAILURE
        if (objectList.isEmpty()) {
            return FAILURE;
        }

        // Otherwise process list as an xml string
        //======================================================================
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("<?xml version=\"1.0\"?>")
                .append("<Grid>")
                .append("<Body>")
                .append("<B>");

        for (Map<String, Object> map : objectList) {
            stringBuilder.append("<I");
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                stringBuilder
                        .append(" ")
                        .append(entry.getKey())
                        .append("=\"")
                        .append(entry.getValue())
                        .append("\"");
            }
            stringBuilder.append("/>");
        }
        stringBuilder.append("</B>")
                .append("</Body>")
                .append("</Grid>");

        return stringBuilder.toString();
    }

    /**
     *
     * @param outputString
     * @param fileName
     */
    public static synchronized void saveStringAsFile(String outputString, String fileName) throws IOException {

        try {
            Sardine sardine = SardineFactory.begin();
            if (sardine.exists(fileName)) {
                sardine.delete(fileName);
            }
            sardine.put(fileName, outputString.getBytes("UTF-8"));

            List<DavResource> resources = sardine.getResources(fileName);
            for (DavResource res : resources) {
                if (res.isDirectory()) {
                    continue;
                } else {
                    Map<String, String> props = res.getCustomProps();
                    if (FAILURE.equalsIgnoreCase(outputString)) {
                        props.put(SUCCESS, FALSE);
                    } else {
                        props.put(SUCCESS, TRUE);
                    }
//                    sardine.put(url, outputString.getBytes("UTF-8"));
                    sardine.setCustomProps(fileName, props, null);
                    
                    break;
                }                
            }
        } catch (UnsupportedEncodingException ex) {
            Exceptions.printStackTrace(ex);
        } catch (SardineException ex) {
            Exceptions.printStackTrace(ex);
        }
    }

    /**
     *
     * @param data
     * @param intVarMap
     * @param solver
     * @return
     */
    public List<Map<String, Object>> updateData(
            List<Map<String, Object>> data,
            Map<String, IntegerVariable> intVarMap,
            OptiBean optiBean,
            Solver solver) {

        if (solver.getNbSolutions() > 0) {
            for (Map<String, Object> item : data) {
                String name = getName(item, optiBean);
                IntegerVariable intVar = intVarMap.get(name);
                item.put(optiBean.PROD_VALUE(), solver.getVar(intVar).getVal());
            }
        }
        return data;
    }

    /**
     *
     * @param map
     * @return
     */
    public static String getName(Map<String, Object> map, OptiBean optiBean) {
        String name = (String) map.get(optiBean.PROD()) + "_"
                + (Integer) map.get(optiBean.PROCESS_STEP()) + "_"
                + (String) map.get(optiBean.PROCESSOR());
        return name;
    }
}
