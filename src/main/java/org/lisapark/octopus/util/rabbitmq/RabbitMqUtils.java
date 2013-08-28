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
package org.lisapark.octopus.util.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.util.List;
import org.json.JSONException;
import org.json.JSONObject;
import org.lisapark.octopus.core.event.Attribute;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.event.EventType;
import org.openide.util.Exceptions;

/**
 *
 * @author Alex
 */
public class RabbitMqUtils {
    
    public static void main(String[] args){
        Boolean running = true;

//        EventType eventType = source.getOutput().getEventType();
//        List<Attribute> attributes = eventType.getAttributes();

        Connection connection = null;
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUsername("guest");
            factory.setPassword("guest");

//            factory.setHost("");
            connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.exchangeDeclare("rpc", "direct");
            channel.queueDeclare("queue_name", false, false, false, null);
            channel.queueBind("queue_name", "rpc", "queue_name");

            QueueingConsumer consumer = new QueueingConsumer(channel);
            
            channel.basicConsume("queue_name", false, "queue_name", consumer);

            while (running) {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery(25000L);

                if (delivery == null) {                    
                    break;
                }

                String message = new String(delivery.getBody());
                System.out.println(message);
                JSONObject jsonobject = new JSONObject(message);
            }
        } catch (JSONException ex) {
            Exceptions.printStackTrace(ex);
        } catch (InterruptedException ex) {
            Exceptions.printStackTrace(ex);
        } catch (ShutdownSignalException ex) {
            Exceptions.printStackTrace(ex);
        } catch (ConsumerCancelledException ex) {
            Exceptions.printStackTrace(ex);
        } catch (IOException ex) {
            Exceptions.printStackTrace(ex);
        } finally {
            running = false;
            try {
                connection.close();
            } catch (Exception ignore) {
            }
        }
    }

}
