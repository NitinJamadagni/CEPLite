package cep.core;

public class TimeEventPair {

	private final Long time;
	private final String[] event;
	public TimeEventPair(long in_time,String[] in_event)
	{
		time=in_time;
		event=in_event;
	}
	
	public Long getTime()
	{
		return time;
	}
	public String[] getEvent()
	{
		return event;
	}
}
