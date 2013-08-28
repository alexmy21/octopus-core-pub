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
import org.lisapark.octopus.core.ProcessingException;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Attribute;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.event.EventType;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.ProcessingRuntime;
import java.util.Map;
import java.util.UUID;

import org.lisapark.octopus.core.source.external.CompiledExternalSource;
import org.lisapark.octopus.core.source.external.ExternalSource;
import static com.google.common.base.Preconditions.checkState;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import net.htmlparser.jericho.Element;
import net.htmlparser.jericho.HTMLElementName;
import net.htmlparser.jericho.MasonTagTypes;
import net.htmlparser.jericho.MicrosoftConditionalCommentTagTypes;
import net.htmlparser.jericho.PHPTagTypes;
import net.htmlparser.jericho.Source;
import org.openide.util.Exceptions;

/**
 * This class is an {@link ExternalSource} that is used to access content of
 * html page on web. It can be configured with a Web Page URL and specific text
 * that indicates beginning and an end of extracted content.
 * <p/>
 *
 * @author dave sinclair(david.sinclair@lisa-park.com)
 * @author alex mylnikov(alexmy@lisa-park.com)
 */
@Persistable
public class HtmlTableSource extends ExternalSource {

    private final static java.util.logging.Logger logger = java.util.logging.Logger.getLogger(HtmlTableSource.class.getName());
    private static final String DEFAULT_NAME = "HTML Source";
    private static final String DEFAULT_DESCRIPTION = "Access to the content of HTML page on Internet.";
    private static final int URL_PARAMETER_ID = 1;
    private static final int TEXT_START_PARAMETER_ID = 2;
    private static final int TEXT_END_PARAMETER_ID = 3;
    private static final int FIELD_COUNT_PARAMETER_ID = 4;
    private static final int ROW_HEADER_PARAMETER_ID = 5;
    private static final int EXCLUDED_WORDS_PARAMETER_ID = 6;
    private static final int FIELD_MAP_PARAMETER_ID = 7;
    private static final String ENTRY_DEL = "&";

    private HtmlTableSource(UUID sourceId, String name, String description) {
        super(sourceId, name, description);
    }

    private HtmlTableSource(UUID sourceId, HtmlTableSource copyFromSource) {
        super(sourceId, copyFromSource);
    }

    private HtmlTableSource(HtmlTableSource copyFromSource) {
        super(copyFromSource);
    }

    public String getUrl() {
        return getParameter(URL_PARAMETER_ID).getValueAsString();
    }

    public String getTextStart() {
        return getParameter(TEXT_START_PARAMETER_ID).getValueAsString();
    }

    public String getTextEnd() {
        return getParameter(TEXT_END_PARAMETER_ID).getValueAsString();
    }

    public Integer getFieldCount() {
        return getParameter(FIELD_COUNT_PARAMETER_ID).getValueAsInteger();
    }

    public Integer getRowHeader() {
        return getParameter(ROW_HEADER_PARAMETER_ID).getValueAsInteger();
    }

    public HashSet<String> getExcludedWords() {
        HashSet<String> set = Sets.newHashSet();
        String[] string = getParameter(EXCLUDED_WORDS_PARAMETER_ID)
                .getValueAsString().split(ENTRY_DEL);
        for (String entry : string) {
            set.add(entry.trim());
        }
        return set;
    }

    public Map<String, String> getFieldMap() {
        HashMap<String, String> map = Maps.newHashMap();
        String[] mapString = getParameter(FIELD_MAP_PARAMETER_ID)
                .getValueAsString().split(ENTRY_DEL);

        for (String entry : mapString) {
            String[] pair = entry.split("=");
            if (pair.length > 1) {
                String htmlName = pair[0].trim();
                String fldName = pair[1].trim();

                if (htmlName != null) {
                    map.put(htmlName, fldName);
                }
            }
        }
        return map;
    }

    public EventType getEventType() {
        return getOutput().getEventType();
    }

    @Override
    public HtmlTableSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new HtmlTableSource(sourceId, this);
    }

    @Override
    public HtmlTableSource copyOf() {
        return new HtmlTableSource(this);
    }

    public static HtmlTableSource newTemplate() {
        UUID sourceId = UUID.randomUUID();
        HtmlTableSource html = new HtmlTableSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        html.addParameter(Parameter.stringParameterWithIdAndName(URL_PARAMETER_ID, "URL:")
                .defaultValue("http://zakupki.gov.ru/pgz/public/action/contracts/info/"
                + "common_info/show?contractInfoId=9456908")
                .required(true));
        html.addParameter(Parameter.stringParameterWithIdAndName(TEXT_START_PARAMETER_ID, "Start Text:")
                .defaultValue("Наименование товара, работ, услуг")
                .required(true)
                .description("Text or Value in the top left cell of Html Table."));
        html.addParameter(Parameter.stringParameterWithIdAndName(TEXT_END_PARAMETER_ID, "End Text:")
                .defaultValue("Информация о поставщиках")
                .required(true)
                .description("The first text right after Html Table that is used as a data source."));

        html.addParameter(Parameter.integerParameterWithIdAndName(FIELD_COUNT_PARAMETER_ID, "Field Count:")
                .required(true)
                .defaultValue(6));

        html.addParameter(Parameter.integerParameterWithIdAndName(ROW_HEADER_PARAMETER_ID, "Row Header #:")
                .required(true)
                .defaultValue(1));

        html.addParameter(Parameter.stringParameterWithIdAndName(FIELD_MAP_PARAMETER_ID, "Field name Map:")
                .required(true)
                .defaultValue("Наименование товара, работ, услуг=name&"
                + "Код продукции=prodcode&"
                + "Единицы измерения=measureunit&"
                + "Цена за единицу=costperunit&"
                + "Количество=qty&"
                + "Сумма=sum"));

        html.addParameter(Parameter.stringParameterWithIdAndName(EXCLUDED_WORDS_PARAMETER_ID, "Excluded words or values:")
                .required(true)
                .defaultValue("Итого:")
                .description("Comma separated case sensitive list of excluded words. Any table row that includes those words "
                + "will be excluded from the data source"));

        html.setOutput(Output.outputWithId(1).setName("Content:"));

        return html;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        validate();

        return new CompiledHtmlContentSource(this.copyOf());
    }

    private static class CompiledHtmlContentSource implements CompiledExternalSource {

        private final HtmlTableSource source;
        private volatile boolean running;

        public CompiledHtmlContentSource(HtmlTableSource source) {
            this.source = source;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) throws ProcessingException {

            // this needs to be atomic, both the check and set
            synchronized (this) {
                checkState(!running, "Source is already processing events. Cannot call processEvents again");
                running = true;
            }
            String sourceUrlString = source.getUrl();

            logger.log(Level.INFO, "sourceUrlString: {0}", sourceUrlString);

            if (sourceUrlString.indexOf(':') == -1) {
                sourceUrlString = "file:" + sourceUrlString;
            }
            MicrosoftConditionalCommentTagTypes.register();
            PHPTagTypes.register();
            PHPTagTypes.PHP_SHORT.deregister(); // remove PHP short tags for this example otherwise they override processing instructions
            MasonTagTypes.register();
            Source htmlSource;
            try {
                htmlSource = new Source(new URL(sourceUrlString));
            } catch (MalformedURLException ex) {
                Exceptions.printStackTrace(ex);
                return;
            } catch (IOException ex) {
                Exceptions.printStackTrace(ex);
                return;
            }

            // Call fullSequentialParse manually as most of the source will be parsed.
            htmlSource.fullSequentialParse();

            List<Element> tdElements = htmlSource.getAllElements(HTMLElementName.TD);

            boolean started = Boolean.FALSE;
            List<String> elemList = Lists.newArrayList();

            int i = 0;
            for (Element tdElement : tdElements) {
                // An element can contain other tags so need to extract the text from it:
                String label = tdElement.getContent().getTextExtractor().toString();
                if (label.trim().equalsIgnoreCase(source.getTextStart()) && !started) {
                    started = Boolean.TRUE;
                }

                if (started) {
                    if (label.trim().equalsIgnoreCase(source.getTextEnd().trim())) {
                        break;
                    } else {
                        elemList.add(label);
                    }
                }
            }

            logger.log(Level.INFO, "elemList: {0}", elemList);

            List<String> headerList = Lists.newArrayList();
            Map<String, String> fldNames = source.getFieldMap();

            logger.log(Level.INFO, "fldNames: {0}", fldNames);

            // Build header List
            for (int j = 0; j < source.getFieldCount(); j++) {
                headerList.add(elemList.get(j));
            }

            List<Map<String, String>> rowList = Lists.newArrayList();

            // Build List of maps        
            HashSet<String> noValues = source.getExcludedWords();
            int k = source.getFieldCount() + 1;
            while (k < elemList.size() - (3 + source.getFieldCount())) {
                Map<String, String> rowMap = Maps.newHashMap();
                // Build Table row map
                for (int l = 0; l < source.getFieldCount(); l++) {
                    rowMap.put(fldNames.get(headerList.get(l)), elemList.get(k));
                    k++;
                }
                HashSet<String> cellValues = extractCellValues(rowMap);
                if (intersect(cellValues, noValues)) {
                    continue;
                }
                rowList.add(rowMap);
            }
            processHtmlTable(rowList, runtime);
        }

        void processHtmlTable(List<Map<String, String>> table, ProcessingRuntime runtime) {
            Thread thread = Thread.currentThread();
            EventType eventType = source.getEventType();

            int i = 0;
            while (!thread.isInterrupted() && running && i < table.size()) {
                Map<String, String> row = table.get(i);
                Event newEvent = createEventFromResultSet(row, eventType);
                i++;

                runtime.sendEventFromSource(newEvent, source);
            }
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }

        Event createEventFromResultSet(Map<String, String> row, EventType eventType) {
            Map<String, Object> attributeValues = Maps.newHashMap();

            for (Attribute attribute : eventType.getAttributes()) {
                Class type = attribute.getType();
                String attributeName = attribute.getName();

                if (type == String.class) {
                    String value = row.get(attributeName);
                    attributeValues.put(attributeName, value);

                } else if (type == Integer.class) {
                    String value = row.get(attributeName).trim().replaceAll("[^\\d.]", "");
                    try {
                        attributeValues.put(attributeName, Integer.parseInt(value));
                    } catch (Exception e) {
                        logger.info(e.getMessage());
                    }

                } else if (type == Short.class) {
                    String value = row.get(attributeName).trim().replaceAll("[^\\d.]", "");
                    try {
                        attributeValues.put(attributeName, Short.parseShort(value));
                    } catch (Exception e) {
                        logger.info(e.getMessage());
                    }

                } else if (type == Long.class) {
                    String value = row.get(attributeName).trim().replaceAll("[^\\d.]", "");
                    try {
                        attributeValues.put(attributeName, Long.parseLong(value));
                    } catch (Exception e) {
                        logger.info(e.getMessage());
                    }

                } else if (type == Double.class) {
                    String value = row.get(attributeName).trim().replaceAll("[^\\d.]", "");
                    try {
                        attributeValues.put(attributeName, Double.parseDouble(value));
                    } catch (Exception e) {
                        logger.info(e.getMessage());
                    }

                } else if (type == Float.class) {
                    String value = row.get(attributeName).trim().replaceAll("[^\\d.]", "");
                    try {
                        attributeValues.put(attributeName, Float.parseFloat(value));
                    } catch (Exception e) {
                        logger.info(e.getMessage());
                    }

                } else if (type == Boolean.class) {
                    String value = row.get(attributeName);
                    try {
                        attributeValues.put(attributeName, Boolean.parseBoolean(value));
                    } catch (Exception e) {
                        logger.info(e.getMessage());
                    }
                } else {
                    throw new IllegalArgumentException(String.format("Unknown attribute type %s", type));
                }
            }

            return new Event(attributeValues);
        }

        private HashSet extractCellValues(Map<String, String> cells) {
            HashSet<String> set = Sets.newHashSet();
            for (Map.Entry<String, String> cell : cells.entrySet()) {
                if (cell != null) {
                    set.add(cell.getValue().trim());
                }
            }
            return set;
        }

        private boolean intersect(HashSet<String> cellValues, HashSet<String> noValues) {
            Boolean retValue = Boolean.FALSE;

            for (String cell : noValues) {
                if (cellValues.contains(cell.trim())) {
                    retValue = Boolean.TRUE;
                    break;
                }
            }

            return retValue;
        }
    }
}
