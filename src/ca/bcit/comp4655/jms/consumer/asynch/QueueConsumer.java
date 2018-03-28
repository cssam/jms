package ca.bcit.comp4655.jms.consumer.asynch;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.NamingException;

import ca.bcit.comp4655.jms.connectionutil.ConnectionElements;
import ca.bcit.comp4655.jms.connectionutil.ConnectionUtil;
import ca.bcit.comp4655.jms.connectionutil.MessagingModelType;

/**
 * Non Durable Msg Consumer 
 * @author Arash Ghavami
 *
 */
public class QueueConsumer
{
	
	public QueueConsumer ( ConnectionElements conElements ) throws NamingException, JMSException
	{
		ConnectionUtil connectionUtil = new ConnectionUtil( conElements );
		connectionUtil.start();
		
		// Receives the messages sent to the destination until the end-of-message-stream control
		while ( true )
		{
			Message m = connectionUtil.getConsumer().receive();
			if ( m != null )
			{
				if ( m instanceof TextMessage ) 
				{
					TextMessage message = ( TextMessage ) m;
					System.out.println( "Reading message: " + message.getText() );
				} 
				else 
				{
					break;
				}
			}
		}
		connectionUtil.disconnect();
	
	}
	
	public static void main( String[] args ) 
	{
		try 
		{
			ConnectionElements connectionElements = new ConnectionElements();
			connectionElements.setDestinationName( "queue/example" );
			connectionElements.setUrl( "localhost:1099" );
			connectionElements.setSessionMode( Session.AUTO_ACKNOWLEDGE );
			connectionElements.setTransactedSession(false);
			connectionElements.setType( MessagingModelType.QUEUE );
			new QueueConsumer( connectionElements );
		} 
		catch (NamingException e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		catch (JMSException e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try 
		{
			Thread.sleep(1200000);
		} 
		catch (InterruptedException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Done listening");
	}

}
