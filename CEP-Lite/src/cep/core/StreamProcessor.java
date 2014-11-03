package cep.core;

import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.json.JSONException;

public class StreamProcessor extends Thread{
	String streamId;
	String queueId;
	InputHandler inputHandler;
	LinkedHashMap<String,String> schemaVariables;
	CEPLiteContext context;
	public void shutDownStream()
	{
		//System.out.println("stooped");
		Thread.currentThread().interrupt();
		
	}
	public void setQueueId(String in_queueId)
	{
		queueId=in_queueId;
		if(in_queueId==null)
		{
			context.addEventQueue(streamId); 
		}
		context.simplethreadpool.process(this);
	}
	public String getQueueId()
	{
		return queueId;
	}
	public StreamProcessor(String name,CEPLiteContext in_context,String... schema) throws lessArgumentsException, UnknownHostException
	{
		streamId=name;
		context=in_context;
		//outputHandler=null;
		schemaVariables=new LinkedHashMap<String,String>();
		//1.loading the schema
			//1.1.Check if empty schema
		if(schema.length==0)
		{
			throw new lessArgumentsException("Schema is null!");
		}
			//1.2.Load it into map
		for(String arg:schema)
		{
			arg=arg.trim();
			String[] tempArgsWithType=arg.split(",");
			 
			for(String arg1:tempArgsWithType)
			{
				String[] tempArgsAndType=arg1.split(" ");
				
				schemaVariables.put(tempArgsAndType[1],tempArgsAndType[0]);
				
			}
		}
		 
		this.setName(streamId); 
		inputHandler=new InputHandler(context,schemaVariables);
		           
	}
	
	public String getStreamId()
	{
		return this.streamId;
	}
	
	public InputHandler getInputHandler()
	{
		return this.inputHandler;
	}
	
	public String getSchema()
	{
		String schemaReturn="";
		for(Map.Entry<String,String> entry:schemaVariables.entrySet())
        {
				schemaReturn=schemaReturn+entry.getKey()+" "+entry.getValue()+",";
        }
		return schemaReturn;
	}
	
	@Override
	public void run()
	{
		//accept query and data input
		
		if(queueId!=null)
		{	
			TimeEventPair event;
			try{
			while(Thread.currentThread().isAlive())
			{
						event=context.pollFromQueue(queueId);
						if(event!=null){		
								
									if(event.getEvent()[0]=="Stop-execution")
									{
										break;
									}
									System.out.println(event.getEvent()[1]); 
									inputHandler.send(event);
								
								//System.out.println("Started polling");	 
						}
				 
			}}
			catch(lessArgumentsException e)
			{
				e.printStackTrace();
			}catch(differentTypesException e) {
				e.printStackTrace();
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		else if(queueId==null)
		{
			TimeEventPair event;
			try{
			while(Thread.currentThread().isAlive())
			{
						
						event=context.pollFromQueue(streamId);
						if(event!=null){//System.out.println(streamId); 		
								
									if(event.getEvent()[0]=="Stop-execution")
									{
										System.out.println("Stopping :"+this.getName()); 
										break;
									}
									inputHandler.send(event);
									 
								
							 
						}
				 
			}}
			catch(lessArgumentsException e)
			{
				e.printStackTrace();
			}catch(differentTypesException e) {
				e.printStackTrace();
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

	
	}
	
	
}
