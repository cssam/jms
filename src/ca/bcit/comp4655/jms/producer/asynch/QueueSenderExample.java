package ca.bcit.comp4655.jms.producer.asynch;

import java.util.Properties;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSession;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;


/**
 * Message Producer class
 * @author Arash Ghavami
 *
 */
public class QueueSenderExample 
{	
	QueueConnection conn = null;
	QueueSession session = null;
	Queue queue = null;
	
	public QueueSenderExample ( String url, String queueName ) throws NamingException, JMSException 
	{
		// Step 1. Create an initial context to perform the JNDI lookup.
		Properties props = new Properties();
		props.setProperty( Context.INITIAL_CONTEXT_FACTORY,"org.jnp.interfaces.NamingContextFactory" );
		props.setProperty( Context.URL_PKG_PREFIXES, "org.jboss.naming:org.jnp.interfaces" );
		props.setProperty( Context.PROVIDER_URL, url );
		Context context = new InitialContext( props );
		
		// Step 2. Lookup the connection factory
		QueueConnectionFactory factory = 
			(QueueConnectionFactory)context.lookup( "ConnectionFactory" );
		
		// Step 3. Lookup the JMS queue
		queue = (Queue)context.lookup(queueName);
		
		// Step 4. Create the JMS objects to connect to the server and manage a session
		conn =  (QueueConnection) factory.createConnection();
		session = conn.createQueueSession( false, QueueSession.AUTO_ACKNOWLEDGE );
		
		conn.start();	
	}

	
	public void disconnect() throws JMSException 
	{
		if (conn != null) 
		{
			conn.stop();
		}

		if (session != null) 
		{
			session.close();
		}

		if (conn != null) 
		{
			conn.close();
		}
	}
	
	private void send( String text ) throws JMSException, NamingException 
	{	
		// Step 5. Create a JMS Message Producer to send a message on the queue
		MessageProducer msgProducer = session.createProducer( queue );
		msgProducer.setDeliveryMode( DeliveryMode.NON_PERSISTENT );
		
		// Step 6. Create a Text Message and send it using the producer
		TextMessage tm = session.createTextMessage( text );
		if ( text!=null )
		{
			msgProducer.send( tm );
		}
		else
		{
			//Sends an empty control message to indicate the end of the message stream:
			msgProducer.send (session.createTextMessage() );
		}
	}
	
	public static void main(String[] args) 
	{
		System.out.println("Sending list of stocks");
		try 
		{
			QueueSenderExample sender =
				new QueueSenderExample ( "localhost:1099", "queue/example" );
						sender.send( "AAPL 10.96");
			sender.send( "ABX 9.08" );
			sender.send( "IBM 12.0" );
			sender.send( "PEF 900.0" );
			sender.send( "Central1 0.0" );
			sender.send( "BBE 2.0" );
			sender.send( "DAW -91.0" );
			//sender.send( null );
			sender.disconnect();
			System.out.println( "JMS Example Sender Complete - list sent" );
		}
		catch (JMSException e)
		{
			e.printStackTrace();
		}
		catch (NamingException e) 
		{
			e.printStackTrace();
		}
	}
}
