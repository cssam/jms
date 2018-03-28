package ca.bcit.comp4655.jms.connectionutil;

public class ConnectionElements
{
	private String destinationName;
	private int sessionMode;
	private boolean transactedSession;
	private MessagingModelType type;
	private String url;
	
	public String getDestinationName() {
		return destinationName;
	}
	public void setDestinationName(String destinationName) {
		this.destinationName = destinationName;
	}
	public int getSessionMode() {
		return sessionMode;
	}
	public void setSessionMode(int sessionMode) {
		this.sessionMode = sessionMode;
	}
	public boolean isTransactedSession() {
		return transactedSession;
	}
	public void setTransactedSession(boolean transactedSession) {
		this.transactedSession = transactedSession;
	}
	public MessagingModelType getType() {
		return type;
	}
	public void setType(MessagingModelType type) {
		this.type = type;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	
}
