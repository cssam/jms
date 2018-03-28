package ca.bcit.comp4655.jms.consumer.synch;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.NamingException;

import ca.bcit.comp4655.jms.connectionutil.ConnectionElements;
import ca.bcit.comp4655.jms.connectionutil.ConnectionUtil;
import ca.bcit.comp4655.jms.connectionutil.MessagingModelType;

public class CalcClient 
{
	public static void main(String[] args) 
	{
		
		try {
			ConnectionUtil requestUtil = new ConnectionUtil( getConnectionElemenets( "/queue/calcRequest" ) );
			//QueueRequestor requestor = new QueueRequestor( (QueueSession) requestUtil.getSession(), (Queue) requestUtil.getDestination() );
			Queue request =( Queue ) requestUtil.getDestination();
			//QueueSender sender = ( (QueueSession) requestUtil.getSession() ).createSender( request );
			MessageProducer sender = requestUtil.getSession().createProducer(request );
			sender.setDeliveryMode( DeliveryMode.NON_PERSISTENT );
			
			MapMessage message = requestUtil.getSession().createMapMessage();
			message.setInt( "operand1", 2 );
			message.setInt( "operand2", 3 );
			message.setString( "operaionName", "Addition" );
			//TextMessage reply = (TextMessage) requestor.request( request );
			System.out.println ( "Sending message " + message.getPropertyNames() );
			
			ConnectionUtil responseUtil = new ConnectionUtil( getConnectionElemenets( "/queue/calcResponse" ) );
			Queue responseQueue = ( Queue ) responseUtil.getDestination();
			message.setJMSReplyTo( responseQueue );
			
			sender.send( message );
			System.out.println ( "Message Sent" );
			
//			ConnectionUtil responseUtil = new ConnectionUtil( getConnectionElemenets( "queue/calcResponse" ) );
//			Queue response = ( Queue ) responseUtil.getDestination();
//			message.setJMSReplyTo( response );
			//QueueReceiver receiver = ( (QueueSession) responseUtil.getSession() ).createReceiver( responseQueue );
			//TextMessage reply = (TextMessage)receiver.receive();
			
			TextMessage reply = (TextMessage) responseUtil.getConsumer().receive();
			if ( reply!=null )
			{
				System.out.println( "2+3=" + reply.getText() );
			}
			else
			{
				System.err.println ( "Calc client timed out!" );
			}
			//requestor.close();
			requestUtil.disconnect();
			responseUtil.disconnect();
		} catch (NamingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
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
