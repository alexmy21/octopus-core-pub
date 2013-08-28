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
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import org.lisapark.octopus.core.Output;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Attribute;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.event.EventType;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.ProcessingRuntime;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.lisapark.octopus.core.processor.impl.PearsonsCorrelationProcessor;
import org.lisapark.octopus.core.source.external.CompiledExternalSource;
import org.lisapark.octopus.core.source.external.ExternalSource;
import org.openide.util.Exceptions;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public class RabbitMqSource extends ExternalSource {

    private static final String DEFAULT_NAME = "RabbitMQ as a source";
    private static final String DEFAULT_DESCRIPTION = "Accepts data from RabbitMQ Server and converts them to the records "
            + " according to the attribute list.";
    
    private static final int USER_NAME_PARAMETER_ID     = 1;
    private static final int PASSWORD_PARAMETER_ID      = 2;
    private static final int RABBITMQ_URL_PARAMETER_ID  = 3;
    private static final int RABBITMQ_PORT_PARAMETER_ID = 4;
    private static final int EXCHANGE_NAME_PARAMETER_ID = 5;
    private static final int QUEUE_NAME_PARAMETER_ID    = 6;
    private static final int TIME_OUT_PARAMETER_ID      = 7;

    public RabbitMqSource(UUID id, String name, String description) {
        super(id, name, description);
    }

    private RabbitMqSource(UUID id, RabbitMqSource copyFromSource) {
        super(id, copyFromSource);
    }

    public RabbitMqSource(RabbitMqSource copyFromSource) {
        super(copyFromSource);
    }

    private String getUserName() {
        return getParameter(USER_NAME_PARAMETER_ID).getValueAsString();
    }

    private String getPassword() {
        return getParameter(PASSWORD_PARAMETER_ID).getValueAsString();
    }

    public String getRabbitMqUrl() {
        return getParameter(RABBITMQ_URL_PARAMETER_ID).getValueAsString();
    }

    private int getRabbitMqPort() {
        String portStr = getParameter(RABBITMQ_PORT_PARAMETER_ID).getValueAsString();
        return (int) Integer.parseInt(portStr.trim().replace(",", "").replace(".", ""));
    }

    public String getExchangeName() {
        return getParameter(EXCHANGE_NAME_PARAMETER_ID).getValueAsString();
    }
    
    public String getQueueName() {
        return getParameter(QUEUE_NAME_PARAMETER_ID).getValueAsString();
    }
    
    private long getTimeOut() {
        return getParameter(TIME_OUT_PARAMETER_ID).getValueAsLong()*1000;
    }

    @Override
    public RabbitMqSource copyOf() {
        return new RabbitMqSource(this);
    }

    @Override
    public RabbitMqSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new RabbitMqSource(sourceId, this);
    }

    public static RabbitMqSource newTemplate() {
        UUID sourceId = UUID.randomUUID();

        RabbitMqSource rabbitSource = new RabbitMqSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        rabbitSource.setOutput(Output.outputWithId(1).setName("Output data"));
        
        rabbitSource.addParameter(
                Parameter.stringParameterWithIdAndName(USER_NAME_PARAMETER_ID, "User name:").
                        description("User name.").
                        defaultValue("guest").
                        required(true)
                );
        rabbitSource.addParameter(
                Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, "Password:").
                        description("Password.").
                        defaultValue("guest").
                        required(true)
                );
        
        rabbitSource.addParameter(
                Parameter.stringParameterWithIdAndName(RABBITMQ_URL_PARAMETER_ID, "Rabbit MQ Server URL:").
                description("Rabbit MQ Server URL.").
                defaultValue("173.72.110.132").
                required(true)
                );

        rabbitSource.addParameter(
                Parameter.stringParameterWithIdAndName(RABBITMQ_PORT_PARAMETER_ID, "Rabbit MQ Server port:").
                description("Rabbit MQ Server port.").
                defaultValue("5672").
                required(true)
                );

        rabbitSource.addParameter(
                Parameter.stringParameterWithIdAndName(EXCHANGE_NAME_PARAMETER_ID, "Rabbit MQ Exchange name:").
                        description("Rabbit MQ Exchange name.").
                        defaultValue("").
                        required(true)
                );
        rabbitSource.addParameter(
                Parameter.stringParameterWithIdAndName(QUEUE_NAME_PARAMETER_ID, "Queue name:").
                        description("Queue name.").
                        defaultValue("queue_name").
                        required(true)
                );
        rabbitSource.addParameter(
                Parameter.longParameterWithIdAndName(TIME_OUT_PARAMETER_ID, "Time Out:").
                        description("Time out in seconds.").
                        defaultValue(10L).
                        required(true)
                );
        return rabbitSource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        return new CompiledTestSource(copyOf());
    }

    static class CompiledTestSource implements CompiledExternalSource {

        private final static Logger logger 
            = java.util.logging.Logger.getLogger(CompiledTestSource.class.getName());
        
        private final RabbitMqSource source;
        private final String QUEUE_NAME;

        /**
         * Running is declared volatile because it may be access my different threads
         */
        private volatile boolean running;
        private final String DIRECT = "direct";

        public CompiledTestSource(RabbitMqSource source) {
            this.source = source;
            this.QUEUE_NAME = source.getQueueName();
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) {
            Thread thread = Thread.currentThread();
            running = true;

            EventType eventType = source.getOutput().getEventType();
            List<Attribute> attributes = eventType.getAttributes();
            
            Connection connection = null;
            Channel channel = null;
            try {
                ConnectionFactory factory = new ConnectionFactory();
                factory.setUsername(source.getUserName());
                factory.setPassword(source.getPassword());
                
                if (!source.getRabbitMqUrl().isEmpty()) {
                    factory.setHost(source.getRabbitMqUrl());
                }
                
                if (source.getRabbitMqPort() > 0) {
                    factory.setPort(source.getRabbitMqPort());
                }
                
                connection = factory.newConnection();
                channel = connection.createChannel();

                channel.queueDeclare(QUEUE_NAME, true, false, false, null);

                String exchname = source.getExchangeName();
                if (!exchname.isEmpty()) {
                    channel.exchangeDeclare(exchname, DIRECT);
                }
                
                channel.queueBind(QUEUE_NAME, exchname, QUEUE_NAME);
                    
                QueueingConsumer consumer = new QueueingConsumer(channel);
                
//                if(exchname.isEmpty()){
//                    channel.basicConsume(QUEUE_NAME, true, consumer);
//                } else {
                    channel.basicConsume(QUEUE_NAME, false, consumer);
//                }
                
                while (true) {
                    QueueingConsumer.Delivery delivery = consumer.nextDelivery(source.getTimeOut());
                                        
                    if (delivery == null || thread.isInterrupted() || !running) {
                        break;
                    }
                    
                    String message = new String(delivery.getBody());
                    logger.log(Level.INFO, message);
                    
                    Event e = createEvent(message, attributes);

                    if(e != null) {
                        runtime.sendEventFromSource(e, source);
                    }
                    
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            } catch (InterruptedException ex) {
                Exceptions.printStackTrace(ex);
            } catch (ShutdownSignalException ex) {
                Exceptions.printStackTrace(ex);
            } catch (ConsumerCancelledException ex) {
                Exceptions.printStackTrace(ex);
            } catch (IOException ex) {
                Exceptions.printStackTrace(ex);
            } finally {
                try {
                    channel.close();
                    connection.close();
                } catch (Exception ignore) {
                }
            }
        }

        private Event createEvent(String message, List<Attribute> attributes) {
            try {
                Map<String, Object> attributeData = Maps.newHashMap();
                JSONObject jsonobject = new JSONObject(message);

                for (Attribute attribute : attributes) {
                    if(jsonobject.has(attribute.getName())) {
                        attributeData.put(attribute.getName(), jsonobject.getString(attribute.getName()));
                    }
                }

                return new Event(attributeData);
            } catch (JSONException ex) {
                logger.log(Level.INFO, message);                
                return null;
            }
        }

        @Override
        public void stopProcessingEvents() {
            running = false;
        }
    }
}
