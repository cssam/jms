package ca.bcit.comp4655.jms.producer.synch;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import javax.jms.DeliveryMode;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import ca.bcit.comp4655.jms.connectionutil.ConnectionElements;
import ca.bcit.comp4655.jms.connectionutil.ConnectionUtil;
import ca.bcit.comp4655.jms.connectionutil.MessagingModelType;

/**
 * The server de-composes the MapMessage to perform the calculation. 
 * It uses an asynchronous message listener and the result is calculated and returned to the ReplyTo destination in the onMessage method.
 * The integers are retrieved by enumerating over the names of the map message.
 *
 */
public class CalcServer
{
	private ConnectionUtil util;
	private QueueSession session;
	public CalcServer() throws NamingException, JMSException
	{
		ConnectionUtil request = new ConnectionUtil( getConnectionElemenets( "queue/calcRequest" ) );
		
		request.start();
		session = (QueueSession) request.getSession();
		
		System.out.println( "Waiting for messages..." );
		MapMessage message = null;
		while ( true )
		{
			 message = (MapMessage) request.getConsumer().receive(10);
			 if ( message !=null )
			 {
				 System.out.println ( "Received Message. Operand 1: " + message.getString("operand1"));
				 System.out.println ( "Received Message. Operand 2: " + message.getString("operand2"));
				 TextMessage outgoingMsg = session.createTextMessage( ""+getResults(message));
		         
				 ConnectionUtil response = new ConnectionUtil( getConnectionElemenets( "queue/calcResponse" ) );
				 Queue responseQueue = ( Queue ) response.getDestination();
				 MessageProducer sender = response.getSession().createProducer(responseQueue );
				 sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT );
				 sender.send( outgoingMsg );
				 
				 //QueueSender sender = session.createSender( ( Queue ) message.getJMSReplyTo() );
				 //QueueSender sender = session.createSender(arg0)
				 break;
			 }
		}
		 
         
//		util = new ConnectionUtil(getConnectionElemenets());
//		QueueReceiver receiver = ((QueueSession ) util.getSession()).createReceiver( (Queue) util.getDestination() );
//		receiver.setMessageListener(this);
//		util.getQueueConnection().setExceptionListener(this);
//		util.disconnect();
//		System.out.println("server listening on " + getConnectionElemenets().getDestinationName() );
	}
	
	public int getResults ( Message message ) throws JMSException
	{
		MapMessage calc = (MapMessage) message;
		OperationType type=null;
		Enumeration e = calc.getMapNames();
		List<Integer> numbers = new ArrayList<Integer>();
		while (e.hasMoreElements())
		{
			String item = (String) e.nextElement();
			if ( item.startsWith( "operand" ) )
			{
				numbers.add( calc.getInt(item));
			}
			else if ( "operaionName".equals( item ) )
			{
				type = OperationType.valueOf( calc.getString( item ) );
			}
		}
		switch ( type )
		{
		case Addition:
			return add( numbers );
		default:
			return -1;
		}
	}
	
	private int add(List<Integer> numbers)
	{
		int total = 0;
		for ( Integer num:numbers )
		{
			total = total + num; 
		}
		return total;
	}

	public static void main(String[] args) 
	{
		try {
			new CalcServer();
		} catch (NamingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
		}
	}
	
	synchronized void waitForever()
	{
	   while (true)
	   {
	      try
	      {
	    	  	Thread.sleep(1000);
			System.out.print( "." );
	      } catch (InterruptedException ex) { }
	   }
	}
	
	
	private enum OperationType
	{
		Addition,
		Subtraction,
		Multiplication,
		Division
	}
	
	private static ConnectionElements getConnectionElemenets( String queueName ) 
	{
		ConnectionElements elements = new ConnectionElements();
		elements.setDestinationName( queueName );
		elements.setSessionMode( Session.DUPS_OK_ACKNOWLEDGE );
		elements.setTransactedSession( false );
		elements.setType( MessagingModelType.QUEUE );
		elements.setUrl( "localhost:1099" );
		return elements;
	}
}
