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

import com.db4o.ta.ActivatableInstrumented;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import org.lisapark.octopus.core.AbstractNode;
import org.lisapark.octopus.core.Input;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.SinkContext;
import org.lisapark.octopus.core.sink.external.CompiledExternalSink;
import org.lisapark.octopus.core.sink.external.ExternalSink;
import org.lisapark.octopus.core.source.Source;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 *
 * @author alex
 */
@Persistable
public class RedisPubSink extends AbstractNode
        implements ExternalSink, ActivatableInstrumented {

    private static final String DEFAULT_NAME = "Redis Sink";
    private static final String DEFAULT_DESCRIPTION = "Works as a Redis Publisher.";
    private static final String DEFAULT_INPUT = "Output.";
    
    private static final int USER_NAME_PARAMETER_ID = 1;
    private static final int PASSWORD_PARAMETER_ID = 2;
    private static final int REDIS_URL_PARAMETER_ID = 3;
    private static final int REDIS_PORT_PARAMETER_ID = 4;
    private static final int CHANNEL_PREFIX_PARAMETER_ID = 5;
    private static final int CHANNEL_NAME_PARAMETER_ID = 6;
    private static final int TIME_OUT_PARAMETER_ID = 7;
    private static final int ATTRIBUTE_LIST_PARAMETER_ID = 8;
    
    private static final String ATTRIBUTE_LIST = "Attribute List";
    private static final String ATTRIBUTE_LIST_DESCRIPTION = "List of comma separated attributes that go to Resin Publisher Server. If list is empty, all attributes go to Resin Publisher Server.";
    private Input<Event> input;

    private RedisPubSink(UUID id, String name, String description) {
        super(id, name, description);
        this.input = Input.eventInputWithId(1);
        this.input.setName("Output.");
        this.input.setDescription("Output.");
    }

    private RedisPubSink(UUID id, RedisPubSink copyFromNode) {
        super(id, copyFromNode);
        this.input = copyFromNode.getInput().copyOf();
    }

    private RedisPubSink(RedisPubSink copyFromNode) {
        super(copyFromNode);
        this.input = copyFromNode.input.copyOf();
    }

    public String getRedisUrl() {
        return getParameter(3).getValueAsString();
    }

    private int getRedisPort() {
        return getParameter(4).getValueAsInteger().intValue();
    }

    private String getUserName() {
        return getParameter(1).getValueAsString();
    }

    private String getPassword() {
        return getParameter(2).getValueAsString();
    }

    public String getChannelPrefix() {
        return getParameter(5).getValueAsString();
    }

    public String getChannelName() {
        return getParameter(6).getValueAsString();
    }

    public String getAttributeList() {
        return getParameter(8).getValueAsString();
    }

    public Input<Event> getInput() {
        return this.input;
    }

    @Override
    public List<Input<Event>> getInputs() {
        return ImmutableList.of(this.input);
    }

    @Override
    public boolean isConnectedTo(Source source) {
        return this.input.isConnectedTo(source);
    }

    @Override
    public void disconnect(Source source) {
        if (this.input.isConnectedTo(source)) {
            this.input.clearSource();
        }
    }

    @Override
    public RedisPubSink newInstance() {
        return new RedisPubSink(UUID.randomUUID(), this);
    }

    @Override
    public RedisPubSink copyOf() {
        return new RedisPubSink(this);
    }

    public static RedisPubSink newTemplate() {

        UUID sinkId = UUID.randomUUID();

        RedisPubSink redisSink = new RedisPubSink(sinkId, "Resin Pub sink", "Works as a Resin Publisher.");

        redisSink.addParameter(Parameter.stringParameterWithIdAndName(1, "User Name:")
                .description("User name.")
                .defaultValue("guest")
                .required(true));

        redisSink.addParameter(Parameter.stringParameterWithIdAndName(2, "Password:")
                .description("Password.")
                .defaultValue("guest")
                .required(true));

        redisSink.addParameter(Parameter.stringParameterWithIdAndName(3, "Redis URL:")
                .description("Redis URL.")
                .defaultValue("localhost")
                .required(true));

        redisSink.addParameter(Parameter.integerParameterWithIdAndName(4, "Redis port:")
                .description("Redis port.")
                .defaultValue(Integer.valueOf(6379))
                .required(true));

        redisSink.addParameter(Parameter.stringParameterWithIdAndName(5, "Redis channel Prefix:")
                .description("Redis List Prefix.")
                .defaultValue("channel")
                .required(true));

        redisSink.addParameter(Parameter.stringParameterWithIdAndName(6, "Redis channel Name:")
                .description("Redis List name.")
                .defaultValue("model")
                .required(true));

        redisSink.addParameter(Parameter.longParameterWithIdAndName(7, "Time Out:")
                .description("Time out in second.")
                .defaultValue(Long.valueOf(10L))
                .required(true));

        redisSink.addParameter(Parameter.stringParameterWithIdAndName(8, "Attribute List")
                .description("List of comma separated attributes that go to Resin Publisher Server."
                + " If list is empty, all attributes go to Resin Publisher Server."));

        return redisSink;
    }

    @Override
    public CompiledExternalSink compile() throws ValidationException {
        return new CompiledResinPub(copyOf());
    }

    static class CompiledResinPub extends CompiledExternalSink {

        private RedisPubSink sink;
        private JedisConnectionFactory cf = null;
        private RedisTemplate<String, String> tmpl;

        protected CompiledResinPub(RedisPubSink processor) {
            super(processor);
            this.sink = processor;
        }

        @Override
        public synchronized void processEvent(SinkContext ctx, Map<Integer, Event> eventsByInputId) {
            Event event = (Event) eventsByInputId.get(Integer.valueOf(1));
            if (event != null) {
                try {
                    if (this.cf == null) {
                        this.cf = new JedisConnectionFactory();
                        this.cf.setHostName(this.sink.getRedisUrl());
                        this.cf.setPort(this.sink.getRedisPort());
                        this.cf.afterPropertiesSet();

                        StringRedisSerializer STRING_SERIALIZER = new StringRedisSerializer();
                        this.tmpl = new RedisTemplate();

                        this.tmpl.setConnectionFactory(this.cf);
                        this.tmpl.setKeySerializer(STRING_SERIALIZER);
                        this.tmpl.setValueSerializer(STRING_SERIALIZER);
                        this.tmpl.afterPropertiesSet();
                    }
                } catch (Exception ex) {
                    ctx.getStandardOut().println(ex);
                }

                String attributeList = this.sink.getAttributeList();

                String outputString = formatOutput(event, attributeList, ctx);
                ctx.getStandardOut().println("Formatted output: ==> " + outputString);

                if (outputString != null) {
                    String channel = this.sink.getChannelPrefix() + this.sink.getChannelName();

                    this.tmpl.opsForList().leftPush(channel, outputString);
                    this.tmpl.convertAndSend(channel, outputString);

                    ctx.getStandardOut().println("outputString is null");
                }
            } else {
                ctx.getStandardOut().println("event is null");
            }
        }

        private String formatOutput(Event event, String attributeList, SinkContext ctx) {
            Map<String, Object> data = event.getData();
            String[] attList = attributeList.split(",");

            Map<String, Object> map = Maps.newHashMap();
            try {
                if (!attributeList.trim().isEmpty()) {
                    for (int i = 0; i < attList.length; i++) {
                        String attr = attList[i];
                        map.put(attr, data.get(attr));
                    }
                } else {
                    for (Entry<String, Object> entry : data.entrySet()) {
                        map.put(entry.getKey(), entry.getValue());
                    }
                }
            } catch (Exception e) {
                ctx.getStandardOut().println(e);
                ctx.getStandardOut().println(event);
            }

            return new Gson().toJson(map, Map.class);
        }
    }
}
