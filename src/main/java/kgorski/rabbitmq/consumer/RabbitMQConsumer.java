package kgorski.rabbitmq.consumer;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * RabbitMQ Consumer.
 * 
 * @author kgorski
 */
public class RabbitMQConsumer extends DefaultConsumer
{
    /**
     * Application entry point.
     * 
     * @param args the application arguments
     * @throws IOException
     * @throws TimeoutException
     */
    public static void main(String[] args) throws IOException, TimeoutException
    {
        // Create connection and channel
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        factory.setAutomaticRecoveryEnabled(true);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        // Take only one message at once
        channel.basicQos(1);
        
        // Run app
        channel.basicConsume("example.queue", false, "myConsumerTag", new RabbitMQConsumer(channel));
    }
    
    /**
     * Class constructor.
     * 
     * @param channel the RabbitMQ channel
     */
    public RabbitMQConsumer(Channel channel) {
        super(channel);
    }
    
    /**
     * {@inheritDoc}
     * 
     * @throws IOException 
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
        throws IOException
    {
        System.out.println("Consumed message: " + new String(body, "UTF-8"));
        getChannel().basicAck(envelope.getDeliveryTag(), false);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig)
    {
        System.out.println("RabbitMQ connection shutdown");
    }
}
