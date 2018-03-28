package ca.bcit.comp4655.jms.connectionutil;

import java.util.Properties;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class ConnectionUtil 
{
	public static String JBOSS_INITIAL_CONTEXT_FACTORY= "org.jnp.interfaces.NamingContextFactory";
	public static String JBOSS_URL_PKG_PREFIXES = "org.jboss.naming:org.jnp.interfaces";
	
	private Destination dest;
	private Session session;
	private QueueConnection queueConnection;
	private TopicConnection topicConnection;
	private MessageConsumer consumer;
	private final Context context;
	
	public QueueConnection getQueueConnection() 
	{
		return queueConnection;
	}

	public TopicConnection getTopicConnection() 
	{
		return topicConnection;
	}
	
	public ConnectionUtil( ConnectionElements conElements ) throws NamingException, JMSException 
	{
		// Step 1. Create an initial context to perform the JNDI lookup
		Properties props = new Properties();
		props.setProperty( Context.INITIAL_CONTEXT_FACTORY,JBOSS_INITIAL_CONTEXT_FACTORY );
		props.setProperty( Context.URL_PKG_PREFIXES, JBOSS_URL_PKG_PREFIXES );
		props.setProperty( Context.PROVIDER_URL, conElements.getUrl() );
		this.context = new InitialContext( props );
	
		switch ( conElements.getType() )
		{
		case QUEUE:
			// lookup the queue connection factory
			QueueConnectionFactory queueFactory = ( QueueConnectionFactory)context.lookup( "/ConnectionFactory" );
			// create a queue connection
			queueConnection = queueFactory.createQueueConnection();
			queueConnection.start();
			dest = (Queue) context.lookup( conElements.getDestinationName() );
			session = queueConnection.createQueueSession( conElements.isTransactedSession(), conElements.getSessionMode() );
			consumer = session.createConsumer( dest );
			break;
		case TOPIC:
			// lookup the topic connection factory
			TopicConnectionFactory topicFactory = ( TopicConnectionFactory )context.lookup( "ConnectionFactory" );
			topicConnection = topicFactory.createTopicConnection();
			session = topicConnection.createTopicSession( conElements.isTransactedSession(), conElements.getSessionMode() );
			dest = (Topic) context.lookup( conElements.getDestinationName() );
			consumer = session.createConsumer( dest );
			break;
		default:
			break;
		}
		
	}
	
	public void start() throws JMSException 
	{
		if ( queueConnection!=null )
		{
			queueConnection.start();
		}
		if ( topicConnection!=null )
		{
			topicConnection.start();
		}
	}

	public Session getSession()
	{
		return session;
	}
	
	public MessageConsumer getConsumer()
	{
		return consumer;
	}
	
	public Destination getDestination()
	{
		return dest;
	}
	
	public void disconnect() throws JMSException 
	{
		
		if ( topicConnection != null) 
		{
			topicConnection.stop();
		}
		if ( queueConnection !=null )
		{
			queueConnection.stop();
		}
		if (session != null) 
		{
			session.close();
		}
		if (topicConnection != null) 
		{
			topicConnection.close();
		}
		if ( queueConnection!=null )
		{
			queueConnection.close();
		}
	}
	
}
