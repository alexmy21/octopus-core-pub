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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lisapark.octopus.core.Output;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Attribute;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.event.EventType;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.ProcessingRuntime;
import org.lisapark.octopus.core.source.external.CompiledExternalSource;
import org.lisapark.octopus.core.source.external.ExternalSource;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Persistable
public class RedisListSource extends ExternalSource {

    private static final String DEFAULT_NAME = "Radis List Source";
    private static final String DEFAULT_DESCRIPTION = "Accepts data from Radis Server and converts them to the records  according to the attribute list.";
    private static final int USER_NAME_PARAMETER_ID = 1;
    private static final int PASSWORD_PARAMETER_ID = 2;
    private static final int REDIS_URL_PARAMETER_ID = 3;
    private static final int REDIS_PORT_PARAMETER_ID = 4;
    private static final int LIST_PREFIX_PARAMETER_ID = 5;
    private static final int LIST_NAME_PARAMETER_ID = 6;

    public RedisListSource(UUID id, String name, String description) {
        super(id, name, description);
    }

    private RedisListSource(UUID id, RedisListSource copyFromSource) {
        super(id, copyFromSource);
    }

    public RedisListSource(RedisListSource copyFromSource) {
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
        String portStr = getParameter(4).getValueAsString();
        return Integer.parseInt(portStr.trim().replace(",", "").replace(".", ""));
    }

    public String getListPrefix() {
        return getParameter(5).getValueAsString();
    }

    public String getListName() {
        return getParameter(6).getValueAsString();
    }

    @Override
    public RedisListSource copyOf() {
        return new RedisListSource(this);
    }

    @Override
    public RedisListSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new RedisListSource(sourceId, this);
    }

    public static RedisListSource newTemplate() {
        UUID sourceId = UUID.randomUUID();

        RedisListSource redisSource = new RedisListSource(sourceId, "Radis List Source", "Accepts data from Radis Server and converts them to the records  according to the attribute list.");
        redisSource.setOutput(Output.outputWithId(1).setName("Output data"));

        redisSource.addParameter(Parameter.stringParameterWithIdAndName(1, "User name:").description("User name.").defaultValue("").required(true));

        redisSource.addParameter(Parameter.stringParameterWithIdAndName(2, "Password:").description("Password.").defaultValue("").required(true));

        redisSource.addParameter(Parameter.stringParameterWithIdAndName(3, "Redis URL:").description("Redis URL.").defaultValue("localhost").required(true));

        redisSource.addParameter(Parameter.stringParameterWithIdAndName(4, "Redis port:").description("Redis port.").defaultValue("6379").required(true));

        redisSource.addParameter(Parameter.stringParameterWithIdAndName(5, "Redis List Prefix:").description("Redis List Prefix.").defaultValue("list").required(true));

        redisSource.addParameter(Parameter.stringParameterWithIdAndName(6, "Redis List Name:").description("Redis List name.").defaultValue("model").required(true));

        return redisSource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        return new CompiledRedisSource(copyOf());
    }

    static class CompiledRedisSource implements CompiledExternalSource {

        private static final Logger logger = Logger.getLogger(CompiledRedisSource.class.getName());
        private final RedisListSource source;
        private final String QUEUE_NAME;
        private volatile boolean running;
        private final String DIRECT = "direct";

        public CompiledRedisSource(RedisListSource source) {
            this.source = source;
            this.QUEUE_NAME = source.getListName();
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) {
            Thread thread = Thread.currentThread();
            this.running = true;

            EventType eventType = this.source.getOutput().getEventType();
            List attributes = eventType.getAttributes();

            JedisConnectionFactory cf = new JedisConnectionFactory();
            cf.setHostName(this.source.getRedisUrl());
            cf.setPort(this.source.getRedisPort());
            cf.afterPropertiesSet();

            StringRedisSerializer STRING_SERIALIZER = new StringRedisSerializer();
            RedisTemplate tmpl = new RedisTemplate();

            tmpl.setConnectionFactory(cf);
            tmpl.setKeySerializer(STRING_SERIALIZER);
            tmpl.setValueSerializer(STRING_SERIALIZER);

            tmpl.afterPropertiesSet();

            String key = this.source.getListPrefix() + this.source.getListName();

            Long size = tmpl.opsForList().size(key);
            Collection<String> messages = tmpl.opsForList().range(key, 0L, size.longValue() - 1L);

            for (String message : messages) {
                if ((thread.isInterrupted()) || (!this.running) || (message == null)) {
                    break;
                }
                Event e = createEvent(message, attributes);
                if (e != null) {
                    runtime.sendEventFromSource(e, this.source);
                }
            }
        }

        private Event createEvent(String message, List<Attribute> attributes) {
            try {
                Map attributeData = Maps.newHashMap();
                Map map = (Map) new Gson().fromJson(message, Map.class);

                for (Attribute attribute : attributes) {
                    if (map.containsKey(attribute.getName())) {
                        attributeData.put(attribute.getName(), map.get(attribute.getName()));
                    }
                }

                return new Event(attributeData);
            } catch (Exception ex) {
                logger.log(Level.INFO, message);
            }
            return null;
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }
    }
}

/* Location:           /home/alex/Documents/
 * Qualified Name:     org.lisapark.octopus.core.source.external.impl.RedisListSource
 * JD-Core Version:    0.6.2
 */