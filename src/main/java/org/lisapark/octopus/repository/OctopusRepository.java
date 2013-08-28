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
package org.lisapark.octopus.repository;

import org.lisapark.octopus.core.ProcessingModel;
import org.lisapark.octopus.core.processor.Processor;
import org.lisapark.octopus.core.sink.external.ExternalSink;
import org.lisapark.octopus.core.source.external.ExternalSource;

import java.util.List;
import java.util.Set;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
public interface OctopusRepository {
    void saveProcessingModel(ProcessingModel model) throws RepositoryException;

    List<ProcessingModel> getProcessingModelsByName(String name) throws RepositoryException;

    ProcessingModel getProcessingModelByName(String name) throws RepositoryException;
    
    List<ExternalSink> getAllExternalSinkTemplates() throws RepositoryException;
    
    Set<ExternalSink> getExternalSinkTemplates(String name) throws RepositoryException;
    
    Set<ExternalSink> getExternalSinkTemplateByClassName(String name) throws RepositoryException;

    List<ExternalSource> getAllExternalSourceTemplates() throws RepositoryException;
    
    Set<ExternalSource> getExternalSourceTemplates(String name) throws RepositoryException;
    
    Set<ExternalSource> getExternalSourceTemplateByClassName(String name) throws RepositoryException;

    List<Processor> getAllProcessorTemplates() throws RepositoryException;
    
    Set<Processor> getProcessorTemplates(String name) throws RepositoryException;
    
    Set<Processor> getProcessorTemplateByClassName(String name) throws RepositoryException;
}
