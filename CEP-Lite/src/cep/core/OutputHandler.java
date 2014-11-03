package cep.core;

import java.net.UnknownHostException;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.JSONObject;


import cep.coap.client.BasicCoapClient;
import cep.coap.client.BasicHttpClient;

public class OutputHandler extends Thread{
	LinkedBlockingQueue<JSONObject> outputQueues;
	BasicCoapClient coapclient;
	BasicHttpClient httpclient;
	private int config=0;
	private static final String HTTP_PORT="8532";
	public static final String HTTP="HTTP";
	public static final String COAP="COAP";
	public OutputHandler(LinkedBlockingQueue<JSONObject> in_outputQueues,String serverAddress) throws UnknownHostException
	{
		outputQueues=in_outputQueues;
		this.setName("outputThread");
		if(serverAddress!=null){
			config=1;
			httpclient=new BasicHttpClient("http://"+serverAddress+":"+HTTP_PORT);}					                //sets the address of the server if its not null
		else if(serverAddress==null)
			config=2;
		this.start();
	}
	public OutputHandler(LinkedBlockingQueue<JSONObject> in_outputQueues,String serverAddress,String in_config) throws UnknownHostException
	{
		outputQueues=in_outputQueues;
		this.setName("outputThread");
		if(in_config.contains("COAP"))	
			coapclient=new BasicCoapClient(serverAddress);                //sets the address of the server if its not null
		else if(in_config.contains("HTTP"))
		{
			config=1;
			httpclient=new BasicHttpClient("http://"+serverAddress+":"+HTTP_PORT);
		}
		else if(serverAddress==null)
			config=2;
		this.start();
	}
	@Override
	public void run()
	{	
		JSONObject temp;
		if(config==1)
		{
			while(true)
			{		
					temp=outputQueues.poll();
					if(temp!=null){
						 
						if(temp.has("Termination")){System.out.println("Stopping this thread");break;}
						System.out.println(temp.toString());
						//httpclient.sendMessage(temp.toString());
						
					}	
			}
		}
		else if(config==0){
			
			while(true)
			{		
					temp=outputQueues.poll();
					if(temp!=null){
						 
						if(temp.has("Termination")){System.out.println("Stopping this thread");break;}
						
						coapclient.sendMessage(temp.toString());
					}	
			}
		}
		
		else if(config==2){
			while(true)
			{		
					temp=outputQueues.poll();
					if(temp!=null){
						 
						if(temp.has("Termination")){break;}
						
						System.out.println(temp.toString());
					}	
			}
		}
	}
}
