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

//import com.google.common.collect.ImmutableList;
//import com.mongodb.DB;
//import com.mongodb.DBCollection;
//import com.mongodb.DBObject;
//import com.mongodb.Mongo;
//import com.mongodb.MongoException;
import com.google.common.collect.ImmutableList;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import net.karmafiles.ff.core.tool.dbutil.converter.Converter;
import org.lisapark.octopus.core.AbstractNode;
import org.lisapark.octopus.core.Input;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.SinkContext;
import org.lisapark.octopus.core.sink.external.CompiledExternalSink;
import org.lisapark.octopus.core.sink.external.ExternalSink;
import org.lisapark.octopus.core.source.Source;
import org.openide.util.Exceptions;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class MongoDbSink  extends AbstractNode implements ExternalSink {
    private static final String DEFAULT_NAME = "MongoDb";
    private static final String DEFAULT_DESCRIPTION = "MongoDb Output";
    private static final String DEFAULT_INPUT = "Input";    
    
    private static final int ATTRIBUTE_URL_PARAMETER_ID = 1;
    private static final String ATTRIBUTE_URL = "MongoDb URL";
    private static final String ATTRIBUTE_URL_DESCRIPTION = "Show Mongo Db URL";

    private Input<Event> input;

    private MongoDbSink(UUID id, String name, String description) {
        super(id, name, description);
        input = Input.eventInputWithId(1);
        input.setName(DEFAULT_INPUT);
        input.setDescription(DEFAULT_INPUT);
    }

    private MongoDbSink(UUID id, MongoDbSink copyFromNode) {
        super(id, copyFromNode);
        input = copyFromNode.getInput().copyOf();
    }

    private MongoDbSink(MongoDbSink copyFromNode) {
        super(copyFromNode);
        this.input = copyFromNode.input.copyOf();
    }
    
    @SuppressWarnings("unchecked")
    public void setUrl(String url) throws ValidationException {
        getParameter(ATTRIBUTE_URL_PARAMETER_ID).setValue(url);
    }

    public String getUrl() {
        return getParameter(ATTRIBUTE_URL_PARAMETER_ID).getValueAsString();
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
    public MongoDbSink newInstance() {
        return new MongoDbSink(UUID.randomUUID(), this);
    }

    @Override
    public MongoDbSink copyOf() {
        return new MongoDbSink(this);
    }

    public static MongoDbSink newTemplate() {
        UUID sinkId = UUID.randomUUID();
        MongoDbSink mongoDbSink = new MongoDbSink(sinkId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        
        mongoDbSink.addParameter(
                Parameter.stringParameterWithIdAndName(ATTRIBUTE_URL_PARAMETER_ID, ATTRIBUTE_URL)
                .description(ATTRIBUTE_URL_DESCRIPTION)
                );
        
        return mongoDbSink;
    }

    @Override
    public CompiledExternalSink compile() throws ValidationException {
        return new CompiledMongoDbSink(copyOf());
    }

    static class CompiledMongoDbSink extends CompiledExternalSink {
        
        private MongoDbSink mongoDbSink; 
        
        protected CompiledMongoDbSink(MongoDbSink processor) {
            super(processor);
            this.mongoDbSink = processor;
        }

        @Override
        public synchronized void processEvent(SinkContext ctx, Map<Integer, Event> eventsByInputId) {
            Event event = eventsByInputId.get(1);
            if (event != null) {
//
//                Mongo mongoDb;
//                try {
//                    mongoDb = new Mongo("localhost", 27017);
//                    DB db = mongoDb.getDB("octopus");
//                    DBCollection coll = db.getCollection("octopus");
//                    coll.drop();
//
//                    DBObject dbObject = Converter.toDBObject(event);
//                    coll.save(dbObject);
//
//                } catch (UnknownHostException ex) {
//                    Exceptions.printStackTrace(ex);
//                } catch (MongoException ex) {
//                    Exceptions.printStackTrace(ex);
//                }

            } else {
                ctx.getStandardOut().println("event is null");
            }
        }
    }
}
