package cep.core;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;



public class CEPManager{
	static HashMap<String,StreamProcessor> streamProcessorMap = null;
	CEPLiteContext context;
	//initialize
    public CEPManager()
	{
    	streamProcessorMap=new HashMap<String,StreamProcessor>();
    	context=new CEPLiteContext();
		
	}
    public final void setServerAddress(String serverAddress)
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
    public void addStreamProcessor(String name,String schema) throws lessArgumentsException, UnknownHostException 
    {
		StreamProcessor stream=new StreamProcessor(name,context,schema); 
		streamProcessorMap.put(name,stream);
	}
    
    public final StreamProcessor getSourceStream(String name)
    {
    	return streamProcessorMap.get(name);
    }
    
    public void shutDownStream(String streamName)
    {
    	if(streamProcessorMap.containsKey(streamName))
    	{
    		streamProcessorMap.get(streamName).shutDownStream();
    		streamProcessorMap.remove(streamName);
    	}

    }
    public void shutDown()
    {
    	for(Entry<String,StreamProcessor> entry:streamProcessorMap.entrySet())
    	{
    		entry.getValue().shutDownStream(); 
    	}
        for(Map.Entry<String,LinkedBlockingQueue<TimeEventPair>> entry:context.pollingQueues.entrySet())
        {
        	while(true)
        	{ 	
        		if(entry.getValue().isEmpty())
        		{
        			entry.getValue().offer(new TimeEventPair(System.currentTimeMillis(),new String[]{"Stop-execution"},""));
        			break;
        		}
        	}
        } 
    	context.shutDown();
    }
}
