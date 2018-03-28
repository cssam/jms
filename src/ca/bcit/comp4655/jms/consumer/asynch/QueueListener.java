package ca.bcit.comp4655.jms.consumer.asynch;


import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.NamingException;

import ca.bcit.comp4655.jms.connectionutil.ConnectionElements;
import ca.bcit.comp4655.jms.connectionutil.ConnectionUtil;
import ca.bcit.comp4655.jms.connectionutil.MessagingModelType;

public class QueueListener implements MessageListener, ExceptionListener
{
	
	@Override
	/**
	 * This method is called asynchronously by JMS when a message arrives at the queue.
	 * Exceptions must be dealt here and not thrown.
	 * @param message A JMS message.
	 */
	public void onMessage( Message message ) 
	{
		TextMessage msg = ( TextMessage ) message;
		try 
       {
          System.out.println("received: " + msg.getText() );
       } 
       catch (JMSException ex) 
       {
          ex.printStackTrace();
       }
       
	}
	
	public QueueListener( ConnectionUtil conUtil ) throws NamingException, JMSException, InterruptedException
	{
		// lookup the queue object
		Queue queue = (Queue) conUtil.getDestination();

		// create a queue connection
		QueueConnection queueConn = conUtil.getQueueConnection();

		// create a non-transacted queue session
		QueueSession queueSession = queueConn.createQueueSession( false,0 );

		// create a queue receiver
		QueueReceiver queueReceiver = queueSession.createReceiver(queue);
		
		// register queueReceiver to *this* message listener 
		queueReceiver.setMessageListener( this );
		
		// set an asynchronous exception listener on the connection
		queueConn.setExceptionListener( this );
		
		// start the connection
		queueConn.start();
		
		// wait for messages
		System.out.print( "waiting for messages" );
		for ( int i = 0; i < 100; i++ ) 
		{
			Thread.sleep(1000);
			System.out.print( "." );
		}
		System.out.println();
	}
	
	public static void main( String[] args )
	{
		//// set the initial context
		ConnectionElements connectionElements = new ConnectionElements();
		connectionElements.setDestinationName( "queue/example" );
		connectionElements.setUrl( "localhost:1099" );
		connectionElements.setSessionMode( Session.AUTO_ACKNOWLEDGE );
		connectionElements.setTransactedSession(false);
		connectionElements.setType( MessagingModelType.QUEUE );
		
		try 
		{
			ConnectionUtil conUtil = new ConnectionUtil( connectionElements );
			new QueueListener( conUtil );
			
		} 
		catch (NamingException e) 
		{
			e.printStackTrace();
		}
		catch (JMSException e) 
		{
			e.printStackTrace();
		} catch (InterruptedException e) 
		{
			e.printStackTrace();
		} 
	}

	@Override
	public void onException(JMSException arg0) 
	{
		// TODO Auto-generated method stub
		System.err.println( "An exception occured: " + arg0.getMessage() );
		arg0.printStackTrace();
		
	}
}
