package cep.core;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.json.JSONException;
import org.json.JSONObject;

public class CEPLiteContext {
	SimpleThreadPool simplethreadpool;
	HashMap<String,LinkedBlockingQueue<TimeEventPair>> pollingQueues;
	LinkedBlockingQueue<JSONObject> outputQueues;
	OutputHandler outputHandler;
	public CEPLiteContext()
	{
		pollingQueues=new HashMap<String,LinkedBlockingQueue<TimeEventPair>>();
		outputQueues=new LinkedBlockingQueue<JSONObject>();
		simplethreadpool=new SimpleThreadPool();
	}
	public void setServerAddress(String serverAddress) throws UnknownHostException
	{
		outputHandler=new OutputHandler(outputQueues,serverAddress);
	}
	public void setServerAddress(String serverAddress,String config)
	{
		try {
			outputHandler=new OutputHandler(outputQueues, serverAddress,config);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void addEventQueue(String EventName)
	{
		pollingQueues.put(EventName, new LinkedBlockingQueue<TimeEventPair>()); 
	}
	public TimeEventPair pollFromQueue(String EventName)
	{
		//if(!pollingQueues.get(EventName).isEmpty())
			//System.out.println(EventName+":"+pollingQueues.get(EventName).toString()); 
		return pollingQueues.get(EventName).poll();
	}
	public void addToOutputQueue(JSONObject object)
	{
		synchronized (this) {
			outputQueues.add(object);
		}
		 
	}
	public void shutDown()
	{
		try {
			outputQueues.add(new JSONObject().put("Termination","terminate!!"));
		} catch (JSONException e) {
			
			e.printStackTrace();
		}
		//simplethreadpool.executor.shutdown();
		 //simplethreadpool.executor.shutdownNow();
		 simplethreadpool.executor.shutdown();

		  try {

		    if (!simplethreadpool.executor.awaitTermination(100, TimeUnit.SECONDS)) {
		    	
		    	simplethreadpool.executor.shutdownNow();

		    }
		  }
		    catch (final InterruptedException pCaught) {

		    	simplethreadpool.executor.shutdownNow();
		    	
		    	Thread.currentThread().interrupt();

		     }
		
		 
	}
}
