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
package org.lisapark.octopus.util.redis;

/**
 *
 * @author alex
 */
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.lisapark.octopus.core.event.Attribute;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.event.EventType;
import org.lisapark.octopus.core.runtime.ProcessingRuntime;
import org.lisapark.octopus.core.source.external.ExternalSource;
import org.openide.util.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

public class RedisMessageListener implements MessageListener {

    private static Logger logger = LoggerFactory.getLogger(RedisMessageListener.class);
    private ProcessingRuntime runtime;
    private ExternalSource source;
    private String quittoken;
    private CountDownLatch latch;
    private RedisMessageListenerContainer container;

    public RedisMessageListener(RedisMessageListenerContainer container, ExternalSource source, ProcessingRuntime runtime, String quittoken, CountDownLatch latch) {
        this.runtime = runtime;
        this.source = source;
        this.quittoken = quittoken;
        this.latch = latch;
        this.container = container;
    }

    @Override
    public void onMessage(Message message, byte[] paramArrayOfByte) {
        EventType eventType = this.source.getOutput().getEventType();
        List attributes = eventType.getAttributes();

        String msg = message.toString().trim();

        if ((msg.startsWith("{")) || (msg.startsWith("["))) {
            Map map = (Map) new Gson().fromJson(msg, Map.class);

            Event e = createEvent(map, attributes);
            if (e != null) {
                this.runtime.sendEventFromSource(e, this.source);
            }
        } else if (this.quittoken.equalsIgnoreCase(msg)) {
            try {
                this.container.stop();
                this.container.destroy();
            } catch (Exception ex) {
                Exceptions.printStackTrace(ex);
            } finally {
                this.latch.countDown();
            }
        }
    }

    private Event createEvent(Map<String, Object> message, List<Attribute> attributes) {
        try {
            Map attributeData = Maps.newHashMap();

            for (Attribute attribute : attributes) {
                if (message.containsKey(attribute.getName())) {
                    attributeData.put(attribute.getName(), message.get(attribute.getName()));
                }
            }

            return new Event(attributeData);
        } catch (Exception ex) {
            logger.info(message.toString());
        }
        return null;
    }
}
