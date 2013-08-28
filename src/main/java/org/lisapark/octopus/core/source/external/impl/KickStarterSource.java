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
import com.google.gson.Gson;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HTTP;
import org.json.JSONException;
import org.json.JSONObject;
import org.lisapark.octopus.core.Output;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.ProcessingRuntime;
import org.lisapark.octopus.core.source.external.CompiledExternalSource;
import org.lisapark.octopus.core.source.external.ExternalSource;
import org.openide.util.Exceptions;

/**
 *
 * @author alex (alexmy@lisa-park.com)
 */
@Persistable
public class KickStarterSource extends ExternalSource {

    private static final String DEFAULT_NAME            = "Model Kickstarter";
    private static final String DEFAULT_DESCRIPTION     = "Kickstarts specified model on Octopus server.";
    
    private static final int OCTOPUS_SERVER_URL_PARAMETER_ID = 1;
    private static final int MODEL_NAME_PARAMETER_ID        = 2;
    private static final int MODEL_PARAM_LIST_PARAMETER_ID  = 3;
    private static final int MODEL_NAME_FIELD_PARAMETER_ID  = 4;
    private static final int PREFIX_NAME_FIELD_PARAMETER_ID = 5;
    private static final int PREFIX_VALUE_PARAMETER_ID      = 6;

    public KickStarterSource(UUID id, String name, String description) {
        super(id, name, description);
    }

    private KickStarterSource(UUID id, KickStarterSource copyFromSource) {
        super(id, copyFromSource);
    }

    public KickStarterSource(KickStarterSource copyFromSource) {
        super(copyFromSource);
    }

    public String getServerUrl() {
        return getParameter(OCTOPUS_SERVER_URL_PARAMETER_ID).getValueAsString();
    }

    public String getModelName() {
        return getParameter(MODEL_NAME_PARAMETER_ID).getValueAsString();
    }

    public String getModelParamJson() {
        return getParameter(MODEL_PARAM_LIST_PARAMETER_ID).getValueAsString();
    }

    public String getModelNameField() {
        return getParameter(MODEL_NAME_FIELD_PARAMETER_ID).getValueAsString();
    }

    public String getParamNameField() {
        return getParameter(PREFIX_NAME_FIELD_PARAMETER_ID).getValueAsString();
    }

    public String getPrefixValue() {
        return getParameter(PREFIX_VALUE_PARAMETER_ID).getValueAsString();
    }

    @Override
    public KickStarterSource copyOf() {
        return new KickStarterSource(this);
    }

    @Override
    public KickStarterSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new KickStarterSource(sourceId, this);
    }

    public static KickStarterSource newTemplate() {
        UUID sourceId = UUID.randomUUID();

        KickStarterSource modelSource = new KickStarterSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        modelSource.setOutput(Output.outputWithId(1).setName("Output data"));

        modelSource.addParameter(Parameter.stringParameterWithIdAndName(OCTOPUS_SERVER_URL_PARAMETER_ID, "OctopusServerURL")
                .description("Octopus server URL including port number.")
                .defaultValue("http://localhost:8080/ModelRunner/run")
                .required(true));

        modelSource.addParameter(Parameter.stringParameterWithIdAndName(MODEL_NAME_PARAMETER_ID, "ModelName")
                .description("Model name as it is in db4o database.")
                .defaultValue("")
                .required(true));

        modelSource.addParameter(Parameter.stringParameterWithIdAndName(MODEL_PARAM_LIST_PARAMETER_ID, "ModelParamJson")
                .description("Model param Json: {paramName1:paramValue1,paramName2:paramValue2, . . . .}")
                .defaultValue("")
                .required(true));

        modelSource.addParameter(Parameter.stringParameterWithIdAndName(MODEL_NAME_FIELD_PARAMETER_ID, "ModelNameField")
                .description("Model name field that is used to output log data.")
                .defaultValue("modelname")
                .required(true));

        modelSource.addParameter(Parameter.stringParameterWithIdAndName(PREFIX_NAME_FIELD_PARAMETER_ID, "ParamNameField")
                .description("Prefix name field that is used by model runner during request analysis.")
                .defaultValue("params")
                .required(true));

//        modelSource.addParameter(Parameter.stringParameterWithIdAndName(PREFIX_VALUE_PARAMETER_ID, "PrefixValue")
//                .description("Model name param prefix value.")
//                .defaultValue("param")
//                .required(true));

        return modelSource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        return new CompiledModelSource(copyOf());
    }

    class CompiledModelSource implements CompiledExternalSource {

        protected final Logger logger = Logger.getLogger(CompiledModelSource.class.getName());
        protected final KickStarterSource source;
        protected volatile boolean running;
        protected Thread thread;

        public CompiledModelSource(KickStarterSource source) {
            this.source = source;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) {
            
             try {
                 
                JSONObject jsonObject = new JSONObject();
                jsonObject.put(source.getModelNameField(), source.getModelName());
                jsonObject.put(source.getParamNameField(), source.getModelParamJson());

                HttpClient client = new DefaultHttpClient();
                HttpPost httpPost = new HttpPost(source.getServerUrl());
                
                httpPost.setHeader("id", source.getName());
                httpPost.setHeader("name", source.getName());
                
                httpPost.setHeader("Content-Type", "application/json");
                StringEntity entity = new StringEntity(jsonObject.toString(), HTTP.UTF_8);
                httpPost.setEntity(entity);

                HttpResponse httpResponse = client.execute(httpPost);

                Map attributeData = Maps.newHashMap();

                attributeData.put(this.source.getModelNameField(), source.getServerUrl());
                attributeData.put("httpResponse", httpResponse);

                runtime.sendEventFromSource(new Event(attributeData), this.source);

            } catch (IOException ex) {
                Exceptions.printStackTrace(ex);
            } catch (IllegalStateException ex) {
                Exceptions.printStackTrace(ex);
            } catch (JSONException ex) {
                Exceptions.printStackTrace(ex);
            }
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }
    }
}
