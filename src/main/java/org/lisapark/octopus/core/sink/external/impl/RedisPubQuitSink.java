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
import java.util.List;
import java.util.Map;
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
 * @author alex (alexmy@lisa-park.com)
 */

@Persistable
public class RedisPubQuitSink extends AbstractNode
        implements ExternalSink, ActivatableInstrumented {

    private static final String DEFAULT_NAME = "Redisn Quit";
    private static final String DEFAULT_DESCRIPTION = "Sends Quit signal to shutdown Redis"
            + " Subscriber on the same channel.";
    private static final String DEFAULT_INPUT = "Output.";
    private static final int USER_NAME_PARAMETER_ID = 1;
    private static final int PASSWORD_PARAMETER_ID = 2;
    private static final int REDIS_URL_PARAMETER_ID = 3;
    private static final int REDIS_PORT_PARAMETER_ID = 4;
    private static final int CHANNEL_PREFIX_PARAMETER_ID = 5;
    private static final int CHANNEL_NAME_PARAMETER_ID = 6;
    private static final int QUIT_SIGNAL_PARAMETER_ID = 7;
    private Input<Event> input;

    private RedisPubQuitSink(UUID id, String name, String description) {
        super(id, name, description);
        this.input = Input.eventInputWithId(1);
        this.input.setName("Output.");
        this.input.setDescription("Output.");
    }

    private RedisPubQuitSink(UUID id, RedisPubQuitSink copyFromNode) {
        super(id, copyFromNode);
        this.input = copyFromNode.getInput().copyOf();
    }

    private RedisPubQuitSink(RedisPubQuitSink copyFromNode) {
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

    public String getQuitSignal() {
        return getParameter(7).getValueAsString();
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
    public RedisPubQuitSink newInstance() {
        return new RedisPubQuitSink(UUID.randomUUID(), this);
    }

    @Override
    public RedisPubQuitSink copyOf() {
        return new RedisPubQuitSink(this);
    }

    public static RedisPubQuitSink newTemplate() {
        UUID sinkId = UUID.randomUUID();

        RedisPubQuitSink redisSink = new RedisPubQuitSink(sinkId, "Resin Pub Quit", "Sends Quit signal to shutdown Resin Subscriber on the same channel.");

        redisSink.addParameter(Parameter.stringParameterWithIdAndName(1, "User Name:").description("User name.").defaultValue("guest").required(true));

        redisSink.addParameter(Parameter.stringParameterWithIdAndName(2, "Password:").description("Password.").defaultValue("guest").required(true));

        redisSink.addParameter(Parameter.stringParameterWithIdAndName(3, "Redis URL:").description("Redis URL.").defaultValue("localhost").required(true));

        redisSink.addParameter(Parameter.integerParameterWithIdAndName(4, "Redis port:").description("Redis port.").defaultValue(Integer.valueOf(6379)).required(true));

        redisSink.addParameter(Parameter.stringParameterWithIdAndName(5, "Redis channel Prefix:").description("Redis List Prefix.").defaultValue("channel").required(true));

        redisSink.addParameter(Parameter.stringParameterWithIdAndName(6, "Redis channel Name:").description("Redis List name.").defaultValue("model").required(true));

        redisSink.addParameter(Parameter.stringParameterWithIdAndName(7, "Quit signal:").defaultValue("quit").required(true));

        return redisSink;
    }

    @Override
    public CompiledExternalSink compile() throws ValidationException {
        return new CompiledRedisQuit(copyOf());
    }

    static class CompiledRedisQuit extends CompiledExternalSink {

        private RedisPubQuitSink sink;
        private JedisConnectionFactory cf = null;
        private RedisTemplate<String, String> tmpl;

        protected CompiledRedisQuit(RedisPubQuitSink processor) {
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

                String quitSignal = this.sink.getQuitSignal();
                ctx.getStandardOut().println("Quit signal output: ==> " + quitSignal);

                if (quitSignal != null) {
                    String channel = this.sink.getChannelPrefix() + this.sink.getChannelName();

                    this.tmpl.convertAndSend(channel, quitSignal);

                    ctx.getStandardOut().println("outputString is null");
                }
            } else {
                ctx.getStandardOut().println("event is null");
            }
        }
    }
}
