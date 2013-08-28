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

import com.google.common.collect.Lists;
import java.util.List;
import org.lisapark.octopus.core.processor.Processor;
import org.lisapark.octopus.core.processor.impl.Addition;
import org.lisapark.octopus.core.processor.impl.And;
import org.lisapark.octopus.core.processor.impl.Crossing;
import org.lisapark.octopus.core.processor.impl.Division;
import org.lisapark.octopus.core.processor.impl.ForecastSRM;
import org.lisapark.octopus.core.processor.impl.LinearRegressionProcessor;
import org.lisapark.octopus.core.processor.impl.Multiplication;
import org.lisapark.octopus.core.processor.impl.Or;
import org.lisapark.octopus.core.processor.impl.PearsonsCorrelationProcessor;
import org.lisapark.octopus.core.processor.impl.PipeDouble;
import org.lisapark.octopus.core.processor.impl.PipeString;
import org.lisapark.octopus.core.processor.impl.PipeStringDouble;
import org.lisapark.octopus.core.processor.impl.Sma;
import org.lisapark.octopus.core.processor.impl.Subtraction;
import org.lisapark.octopus.core.processor.impl.Xor;
import org.lisapark.octopus.core.sink.external.ExternalSink;
import org.lisapark.octopus.core.sink.external.impl.ChocoSimppGssSink;
import org.lisapark.octopus.core.sink.external.impl.ConsoleSink;
import org.lisapark.octopus.core.sink.external.impl.DatabaseSink;
import org.lisapark.octopus.core.sink.external.impl.ForecastGssSink;
import org.lisapark.octopus.core.sink.external.impl.GssSink;
import org.lisapark.octopus.core.sink.external.impl.MachineRels2Neo4jSrvSink;
import org.lisapark.octopus.core.sink.external.impl.MachineResources2Neo4jSrvSink;
import org.lisapark.octopus.core.sink.external.impl.ModelJson2Neo4jEmbSink;
import org.lisapark.octopus.core.sink.external.impl.ModelJson2Neo4jSrvSink;
import org.lisapark.octopus.core.sink.external.impl.ProcessorJson2Neo4jSrvSink;
import org.lisapark.octopus.core.sink.external.impl.ProductRels2Neo4jSrvSink;
import org.lisapark.octopus.core.sink.external.impl.Products2Neo4jSrvSink;
import org.lisapark.octopus.core.sink.external.impl.RabbitMqSink;
import org.lisapark.octopus.core.sink.external.impl.RedisPubQuitSink;
import org.lisapark.octopus.core.sink.external.impl.RedisPubSink;
import org.lisapark.octopus.core.sink.external.impl.TechnologyRelsNeo4jSrvSink;
import org.lisapark.octopus.core.sink.external.impl.TechnologySteps2Neo4jSrvSink;
import org.lisapark.octopus.core.source.external.ExternalSource;
import org.lisapark.octopus.core.source.external.impl.Db4oModelsSource;
import org.lisapark.octopus.core.source.external.impl.Db4oProcessorsSource;
import org.lisapark.octopus.core.source.external.impl.Db4oReplicaSource;
import org.lisapark.octopus.core.source.external.impl.Db4oSinksSource;
import org.lisapark.octopus.core.source.external.impl.Db4oSourcesSource;
import org.lisapark.octopus.core.source.external.impl.GssListSourceQuery;
import org.lisapark.octopus.core.source.external.impl.GssListSourceRange;
import org.lisapark.octopus.core.source.external.impl.HtmlTableSource;
import org.lisapark.octopus.core.source.external.impl.KickStarterSource;
import org.lisapark.octopus.core.source.external.impl.Neo4jMachineResourceSource;
import org.lisapark.octopus.core.source.external.impl.Neo4jOptiSource;
import org.lisapark.octopus.core.source.external.impl.Neo4jProductSource;
import org.lisapark.octopus.core.source.external.impl.Neo4jSinkContextSource;
import org.lisapark.octopus.core.source.external.impl.Neo4jTechnologyStepsSource;
import org.lisapark.octopus.core.source.external.impl.RabbitMqSource;
import org.lisapark.octopus.core.source.external.impl.RedisListSource;
import org.lisapark.octopus.core.source.external.impl.RedisMessageSource;
import org.lisapark.octopus.core.source.external.impl.RedisQuittokenSource;
import org.lisapark.octopus.core.source.external.impl.SimppGssSource;
import org.lisapark.octopus.core.source.external.impl.SqlQuerySource;
import org.lisapark.octopus.core.source.external.impl.TestSource;

public abstract class AbstractOctopusRepository
        implements OctopusRepository {

    @Override
    public List<ExternalSink> getAllExternalSinkTemplates() {
        return Lists.newArrayList(new ExternalSink[]{
                    ConsoleSink.newTemplate(),
                    RabbitMqSink.newTemplate(),
                    RedisPubSink.newTemplate(),
                    RedisPubQuitSink.newTemplate(),
                    DatabaseSink.newTemplate(),
                    GssSink.newTemplate(),
                    ForecastGssSink.newTemplate(),
                    ChocoSimppGssSink.newTemplate(),
                    ProcessorJson2Neo4jSrvSink.newTemplate(),
                    Products2Neo4jSrvSink.newTemplate(),
                    ProductRels2Neo4jSrvSink.newTemplate(),
                    MachineResources2Neo4jSrvSink.newTemplate(),
                    MachineRels2Neo4jSrvSink.newTemplate(),
                    TechnologySteps2Neo4jSrvSink.newTemplate(),
                    TechnologyRelsNeo4jSrvSink.newTemplate(),
                    ModelJson2Neo4jSrvSink.newTemplate(),
                    ModelJson2Neo4jEmbSink.newTemplate()});
    }

    @Override
    public List<ExternalSource> getAllExternalSourceTemplates() {
        return Lists.newArrayList(new ExternalSource[]{
                    KickStarterSource.newTemplate(),
                    RabbitMqSource.newTemplate(),
                    HtmlTableSource.newTemplate(),
                    GssListSourceRange.newTemplate(),
                    GssListSourceQuery.newTemplate(),
                    SimppGssSource.newTemplate(),
                    SqlQuerySource.newTemplate(),
                    Db4oModelsSource.newTemplate(),
                    Db4oSourcesSource.newTemplate(),
                    Db4oSinksSource.newTemplate(),
                    Db4oProcessorsSource.newTemplate(),
                    Db4oReplicaSource.newTemplate(),
                    Neo4jMachineResourceSource.newTemplate(),
                    Neo4jProductSource.newTemplate(),
                    Neo4jSinkContextSource.newTemplate(),
                    Neo4jTechnologyStepsSource.newTemplate(),
                    Neo4jOptiSource.newTemplate(),
                    RedisListSource.newTemplate(),
                    RedisMessageSource.newTemplate(),
                    RedisQuittokenSource.newTemplate(),
                    TestSource.newTemplate()});
    }

    @Override
    public List<Processor> getAllProcessorTemplates() {
        return Lists.newArrayList(new Processor[]{
                    Addition.newTemplate(),
                    And.newTemplate(),
                    Or.newTemplate(),
                    Xor.newTemplate(),
                    Crossing.newTemplate(),
                    ForecastSRM.newTemplate(),
                    Division.newTemplate(),
                    LinearRegressionProcessor.newTemplate(),
                    Multiplication.newTemplate(),
                    PearsonsCorrelationProcessor.newTemplate(),
                    PipeDouble.newTemplate(),
                    PipeString.newTemplate(),
                    PipeStringDouble.newTemplate(),
                    Sma.newTemplate(),
                    Subtraction.newTemplate()});
    }
}
