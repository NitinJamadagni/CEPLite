package cep.core;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;

public class CEPManager{
	static HashMap<String,SourceStream> sourceStreamMap = null;
	CEPLiteContext context;
	//initialize
    public CEPManager()
	{
    	sourceStreamMap=new HashMap<String,SourceStream>();
    	context=new CEPLiteContext();
		
	}
    public void setServerAddress(String serverAddress)
    {
    	try {
			context.setServerAddress(serverAddress);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    }
    public final void setServerAddress(String serverAddress,String config)
    {
    	context.setServerAddress(serverAddress,config);
    }
	public void createEventQueue(String eventId)
	{
		context.addEventQueue(eventId); 
	}
	public void addEventToQueue(String eventId,TimeEventPair eventpair)
	{
		context.pollingQueues.get(eventId).offer(eventpair); 
	}
    public void addStreamSource(String name,String schema) throws lessArgumentsException, UnknownHostException 
    {
		SourceStream stream=new SourceStream(name,context,schema); 
		sourceStreamMap.put(name,stream);
	}
    
    public SourceStream getSourceStream(String name)
    {
    	return sourceStreamMap.get(name);
    }
    
    public void shutDownStream(String streamName)
    {
    	if(sourceStreamMap.containsKey(streamName))
    	{
    		sourceStreamMap.get(streamName).shutDownStream();
    		sourceStreamMap.remove(streamName);
    	}

    }
    public void shutDown()
    {
    	for(Entry<String,SourceStream> entry:sourceStreamMap.entrySet())
    	{
    		entry.getValue().shutDownStream(); 
    	}
        for(Map.Entry<String,LinkedBlockingQueue<TimeEventPair>> entry:context.pollingQueues.entrySet())
        {
        	while(true)
        	{ 
        		if(entry.getValue().isEmpty())
        		{
        			entry.getValue().offer(new TimeEventPair(System.currentTimeMillis(),new String[]{"Stop-execution"}));
        			break;
        		}
        	}
        } 
    	context.shutDown();
    }
}
