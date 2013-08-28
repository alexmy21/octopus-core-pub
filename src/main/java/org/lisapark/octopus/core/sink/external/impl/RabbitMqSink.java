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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.QueueingConsumer;
import org.lisapark.octopus.core.AbstractNode;
import org.lisapark.octopus.core.Input;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.runtime.SinkContext;
import org.lisapark.octopus.core.source.Source;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.logging.Level;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.json.JSONWriter;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.sink.external.CompiledExternalSink;
import org.lisapark.octopus.core.sink.external.ExternalSink;
import org.lisapark.octopus.util.jdbc.Connections;
import org.openide.util.Exceptions;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public class RabbitMqSink extends AbstractNode implements ExternalSink {
    
    private static final String DEFAULT_NAME = "RabbitMQ Out";
    private static final String DEFAULT_DESCRIPTION = "Output to Rabbit MQ Server";
    private static final String DEFAULT_INPUT = "Output.";    
    
    private static final int USER_NAME_PARAMETER_ID         = 1;
    private static final int PASSWORD_PARAMETER_ID          = 2;
    private static final int RABBITMQ_URL_PARAMETER_ID      = 3;
    private static final int RABBITMQ_PORT_PARAMETER_ID     = 4;
    private static final int EXCHANGE_NAME_PARAMETER_ID     = 5;
    private static final int QUEUE_NAME_PARAMETER_ID        = 6;
    private static final int ATTRIBUTE_LIST_PARAMETER_ID    = 7;
    private static final int TIME_OUT_PARAMETER_ID          = 8;
    
    private static final String ATTRIBUTE_LIST = "Attribute List";
    private static final String ATTRIBUTE_LIST_DESCRIPTION = 
            "List of comma separated attributes that go to Rabbit MQ Server."
            + " If list is empty, all attributes go to Rabbit MQ Server.";
    
    private Input<Event> input;

    private RabbitMqSink(UUID id, String name, String description) {
        super(id, name, description);
        input = Input.eventInputWithId(1);
        input.setName(DEFAULT_INPUT);
        input.setDescription(DEFAULT_INPUT);
    }

    private RabbitMqSink(UUID id, RabbitMqSink copyFromNode) {
        super(id, copyFromNode);
        input = copyFromNode.getInput().copyOf();
    }

    private RabbitMqSink(RabbitMqSink copyFromNode) {
        super(copyFromNode);
        this.input = copyFromNode.input.copyOf();
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

    public int getRabbitMqPort() {
       
        Object object = getParameter(RABBITMQ_PORT_PARAMETER_ID).getValue();
//        if (object instanceof String) {
//            String portStr = (String) object;
            return (int) Integer.parseInt(object.toString());
//                    portStr.trim().replace(",", "")                    
//                    .replace(".", ""));
//        } else {
//            return (int)getParameter(RABBITMQ_PORT_PARAMETER_ID).getValueAsInteger();
//        }
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

    public String getAttributeList() {
        return getParameter(ATTRIBUTE_LIST_PARAMETER_ID).getValueAsString();
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
    public RabbitMqSink newInstance() {
        return new RabbitMqSink(UUID.randomUUID(), this);
    }

    @Override
    public RabbitMqSink copyOf() {
        return new RabbitMqSink(this);
    }

    public static RabbitMqSink newTemplate() {
        UUID sinkId = UUID.randomUUID();
        RabbitMqSink rabbitSink = new RabbitMqSink(sinkId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        
        rabbitSink.addParameter(
                Parameter.stringParameterWithIdAndName(USER_NAME_PARAMETER_ID, "User Name:").
                        description("User name.").
                        defaultValue("guest").
                        required(true)
                );
        rabbitSink.addParameter(
                Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, "Password:").
                        description("Password.").
                        defaultValue("guest").
                        required(true)
                );
        
        rabbitSink.addParameter(
                Parameter.stringParameterWithIdAndName(RABBITMQ_URL_PARAMETER_ID, "Rabbit MQ Server URL:").
                        description("Rabbit MQ Server URL.").
                        defaultValue("173.72.110.132").
                        required(true)
                );
        
        rabbitSink.addParameter(
                Parameter.stringParameterWithIdAndName(RABBITMQ_PORT_PARAMETER_ID, "Rabbit MQ Server port:").
                description("Rabbit MQ Server Port.").
                defaultValue("5672").
                required(true)
                );

        rabbitSink.addParameter(
                Parameter.stringParameterWithIdAndName(EXCHANGE_NAME_PARAMETER_ID, "Rabbit MQ Exchange name:").
                        description("Rabbit MQ Exchange name.").
                        defaultValue("").
                        required(true)
                );
        rabbitSink.addParameter(
                Parameter.stringParameterWithIdAndName(QUEUE_NAME_PARAMETER_ID, "Queue name:").
                        description("Queue name.").
                        defaultValue("queue_name").
                        required(true)
                );
        rabbitSink.addParameter(
                Parameter.longParameterWithIdAndName(TIME_OUT_PARAMETER_ID, "Time Out:").
                        description("Time out in second.").
                        defaultValue(10L).
                        required(true)
                );
        rabbitSink.addParameter(
                Parameter.stringParameterWithIdAndName(ATTRIBUTE_LIST_PARAMETER_ID, ATTRIBUTE_LIST)
                .description(ATTRIBUTE_LIST_DESCRIPTION)
                );
        
        return rabbitSink;
    }

    @Override
    public CompiledExternalSink compile() throws ValidationException {
        return new CompiledRabbitMq(copyOf());
    }

    static class CompiledRabbitMq extends CompiledExternalSink {
        
        private RabbitMqSink sink; 
        private RabbitMqClient client;
        
        protected CompiledRabbitMq(RabbitMqSink processor) {
            super(processor);
            this.sink = processor;            
        }

        @Override
        public synchronized void processEvent(SinkContext ctx, Map<Integer, Event> eventsByInputId) {
            Event event = eventsByInputId.get(1);
            if (event != null) {
                
                if(client == null){
                    try {
                        client = new RabbitMqClient(
                                sink.getUserName(),
                                sink.getPassword(),
                                sink.getRabbitMqUrl(),
                                sink.getRabbitMqPort(),
                                sink.getExchangeName(),
                                sink.getQueueName()
                                ).init();
                    } catch (Exception ex) {
                        ctx.getStandardOut().println(ex);
                        return;
                    }
                }                
                String attributeList = sink.getAttributeList();

                // Print out attributes
                String outputString = formatOutput(event, attributeList, ctx);
                ctx.getStandardOut().println("Formatted output: ==> " + outputString);
                try {
                    // Send message to Rabbit MQ Server
                    client.send(outputString);
                } catch (Exception ex) {
                    ctx.getStandardOut().println(ex);
                }
                // Print out only if there are some attributes are not null
                if (outputString == null) {
                    ctx.getStandardOut().println("outputString is null");
                }
            } else {
                ctx.getStandardOut().println("event is null");
            }
        }

        private String formatOutput(Event event, String attributeList, SinkContext ctx) {
            
            Map<String, Object> data = event.getData();
            String[] attList = attributeList.split(",");

            String retString = null;
            try {
                JSONObject object = new JSONObject();
                if (!attributeList.trim().isEmpty()) {
                    
                    for (int i = 0; i < attList.length; i++) {
                        String attr = attList[i];
                        object.put(attr, data.get(attr).toString());
                    }
                } else {
                    for(Entry<String, Object> entry : data.entrySet()){
                        object.put(entry.getKey(), entry.getValue().toString());
                    }
                }
                retString = object.toString();
            } catch (JSONException e) {
                ctx.getStandardOut().println(e);
                ctx.getStandardOut().println(event);
            }
            
            return retString;            
        }        
        
        @Override
        protected void finalize() throws Throwable{            
            super.finalize();  
            if (client != null) {
                try {
                    client.close();
                } catch (Exception ignore) {
                }
            }
        }
    }
    
    static class RabbitMqClient {

        private Connection connection;
        private Channel channel;
        
        private String userName;
        private String password;
        private String url;
        private int port;
        private String exchangeName;
        private String queueName;
        
        private static final String DIRECT = "direct";
        
        public RabbitMqClient(String userName, String password, String url, int port, 
                String exchangeName, String queueName){
            this.userName       = userName;
            this.password       = password;
            this.url            = url;
            this.port           = port;
            this.exchangeName   = exchangeName;
            this.queueName      = queueName;
        }
        
        public RabbitMqClient init() throws Exception {

            ConnectionFactory factory = new ConnectionFactory();
            factory.setUsername(userName);
            factory.setPassword(password);
            factory.setHost(url);
            factory.setPort(port);
                
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.exchangeDeclare(exchangeName, DIRECT);
            channel.queueDeclare(queueName, true, false, false, null);
            channel.queueBind(queueName, exchangeName, queueName);
            return this;
        }
        
        public String send(String message) throws Exception {
            String response = null;
            channel.basicPublish(exchangeName, queueName,
                    MessageProperties.TEXT_PLAIN, 
                    message.getBytes());
            return response;
        }
        
        public void close() throws Exception {
            connection.close();
        }
        
//        private AMQP.BasicProperties getRequestProperties() {
//            return new AMQP.BasicProperties.Builder().replyTo(replyQueueName).build();
//        }
    }
}
