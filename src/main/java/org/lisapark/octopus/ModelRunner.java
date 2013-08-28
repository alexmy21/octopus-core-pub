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
package org.lisapark.octopus;

import java.io.PrintStream;
import java.util.Map;
import java.util.Set;
import org.lisapark.octopus.core.ProcessingModel;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.compiler.esper.EsperCompiler;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.ProcessingRuntime;
import org.lisapark.octopus.core.sink.external.ExternalSink;
import org.lisapark.octopus.core.source.external.ExternalSource;
import org.openide.util.Exceptions;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class ModelRunner {
    
    private ProcessingModel model;
    
    public ModelRunner(ProcessingModel model){
        this.model = model;        
    }
    
    public ModelRunner(ProcessingModel model, 
            Map<String, String> sourceParam, 
            Map<String, String> sinkParam){
        
        this.model = model; 
        if (sourceParam != null) {
            Set<ExternalSource> extSources = this.model.getExternalSources();
            for (ExternalSource extSource : extSources) {
                Set<Parameter> params = extSource.getParameters();
                for (Parameter param : params) {
                    String paramName = param.getName();
                    if (sourceParam.containsKey(paramName)
                            && sourceParam.get(paramName) != null) {
                        try {
                            param.setValueFromString(sourceParam.get(paramName));
                        } catch (ValidationException ex) {
                            Exceptions.printStackTrace(ex);
                        }
                    }
                }
            }
        }
        
        if (sinkParam != null) {
            Set<ExternalSink> extSinks = this.model.getExternalSinks();
            for (ExternalSink extSink : extSinks) {
                Set<Parameter> params = extSink.getParameters();
                for (Parameter param : params) {
                    String paramName = param.getName();
                    if (sinkParam.containsKey(paramName)
                            && sinkParam.get(paramName) != null) {
                        try {
                            param.setValueFromString(sinkParam.get(paramName));
                        } catch (ValidationException ex) {
                            Exceptions.printStackTrace(ex);
                        }
                    }
                }
            }
        }
    }
    
    /**
     * 
     * @param currentProcessingModel 
     */
    public void runModel() {
        
        if (model != null) {
            org.lisapark.octopus.core.compiler.Compiler compiler = new EsperCompiler();
            PrintStream stream = new PrintStream(System.out);
            compiler.setStandardOut(stream);
            compiler.setStandardError(stream);
            
            try {
                ProcessingRuntime runtime = compiler.compile(model);
                
                runtime.start();
                runtime.shutdown();
            } catch (ValidationException e1) {
                System.out.println(e1.getLocalizedMessage() + "\n");
            }
        }
    }
}
