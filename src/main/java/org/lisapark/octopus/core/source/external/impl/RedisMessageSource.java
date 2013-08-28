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

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;
import org.lisapark.octopus.core.Output;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.ProcessingRuntime;
import org.lisapark.octopus.core.source.external.CompiledExternalSource;
import org.lisapark.octopus.core.source.external.ExternalSource;
import org.lisapark.octopus.util.redis.RedisMessageListener;
import org.openide.util.Exceptions;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

/**
 *
 * @author alex
 */
@Persistable
public class RedisMessageSource extends ExternalSource {

    private static final String DEFAULT_NAME = "Radis Message Source";
    private static final String DEFAULT_DESCRIPTION = "Accepts data from Radis Server and converts them to the records  according to the attribute list.";
    private static final int USER_NAME_PARAMETER_ID = 1;
    private static final int PASSWORD_PARAMETER_ID = 2;
    private static final int REDIS_URL_PARAMETER_ID = 3;
    private static final int REDIS_PORT_PARAMETER_ID = 4;
    private static final int CHANNEL_PREFIX_PARAMETER_ID = 5;
    private static final int CHANNEL_NAME_PARAMETER_ID = 6;
    private static final int TIME_OUT_PARAMETER_ID = 7;
    private static final int QUIT_SIGNAL_PARAMETER_ID = 8;

    public RedisMessageSource(UUID id, String name, String description) {
        super(id, name, description);
    }

    private RedisMessageSource(UUID id, RedisMessageSource copyFromSource) {
        super(id, copyFromSource);
    }

    public RedisMessageSource(RedisMessageSource copyFromSource) {
        super(copyFromSource);
    }

    private String getUserName() {
        return getParameter(1).getValueAsString();
    }

    private String getPassword() {
        return getParameter(2).getValueAsString();
    }

    public String getRedisUrl() {
        return getParameter(3).getValueAsString();
    }

    private int getRedisPort() {
        return getParameter(4).getValueAsInteger().intValue();
    }

    public String getChannelPrefix() {
        return getParameter(5).getValueAsString();
    }

    public String getChannelName() {
        return getParameter(6).getValueAsString();
    }

    private long getTimeOut() {
        return getParameter(7).getValueAsLong().longValue() * 1000L;
    }

    public String getQuitSignal() {
        return getParameter(8).getValueAsString();
    }

    @Override
    public RedisMessageSource copyOf() {
        return new RedisMessageSource(this);
    }

    @Override
    public RedisMessageSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new RedisMessageSource(sourceId, this);
    }

    public static RedisMessageSource newTemplate() {
        UUID sourceId = UUID.randomUUID();

        RedisMessageSource redisSource = new RedisMessageSource(sourceId, "Radis Message Source", "Accepts data from Radis Server and converts them to the records  according to the attribute list.");
        redisSource.setOutput(Output.outputWithId(1).setName("Output data"));

        redisSource.addParameter(Parameter.stringParameterWithIdAndName(1, "User name:").description("User name.").defaultValue("").required(true));

        redisSource.addParameter(Parameter.stringParameterWithIdAndName(2, "Password:").description("Password.").defaultValue("").required(true));

        redisSource.addParameter(Parameter.stringParameterWithIdAndName(3, "Redis URL:").description("Redis URL.").defaultValue("localhost").required(true));

        redisSource.addParameter(Parameter.integerParameterWithIdAndName(4, "Redis port:").description("Redis port.").defaultValue(Integer.valueOf(6379)).required(true));

        redisSource.addParameter(Parameter.stringParameterWithIdAndName(5, "Redis List Prefix:").description("Redis List Prefix.").defaultValue("channel").required(true));

        redisSource.addParameter(Parameter.stringParameterWithIdAndName(6, "Redis List Name:").description("Redis List name.").defaultValue("model").required(true));

        redisSource.addParameter(Parameter.integerParameterWithIdAndName(7, "Time out:").description("Time out in seconds.").defaultValue(Integer.valueOf(30)).required(true));

        redisSource.addParameter(Parameter.stringParameterWithIdAndName(8, "Quit signal token:").description("Quit signal token.").defaultValue("quit").required(true));

        return redisSource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        return new CompiledRedisSource(copyOf());
    }

    class CompiledRedisSource implements CompiledExternalSource {

        protected final Logger logger = Logger.getLogger(CompiledRedisSource.class.getName());
        protected final RedisMessageSource source;
        protected volatile boolean running;
        protected Thread thread;

        public CompiledRedisSource(RedisMessageSource source) {
            this.source = source;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) {
            CountDownLatch latch = new CountDownLatch(1);
            try {
                this.thread = Thread.currentThread();
                this.running = true;

                String channel = this.source.getChannelPrefix() + this.source.getChannelName();

                JedisConnectionFactory cf = new JedisConnectionFactory();
                cf.setHostName(this.source.getRedisUrl());
                cf.setPort(this.source.getRedisPort());
                cf.afterPropertiesSet();

                RedisMessageListenerContainer container = new RedisMessageListenerContainer();
                container.setConnectionFactory(cf);

                container.addMessageListener(new MessageListenerAdapter(new RedisMessageListener(container, this.source, runtime, this.source.getQuitSignal(), latch)), new ChannelTopic(channel));

                container.afterPropertiesSet();

                container.start();

                latch.await();
            } catch (InterruptedException ex) {
                Exceptions.printStackTrace(ex);
            }
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }
    }
}
