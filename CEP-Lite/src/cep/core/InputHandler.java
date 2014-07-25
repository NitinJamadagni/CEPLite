package cep.core;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.json.JSONException;
import org.json.JSONObject;
import cep.query.booleanQuery.*;
import cep.query.patternQuery.PatternQuery;
import cep.query.patternQuery.continuousSequencePatternQuery;
import cep.query.patternQuery.sequenceInWindowPatternQuery;
import cep.query.Query;


public class InputHandler {
		private ArrayList<Query> queries;
		private LinkedHashMap<String,String> schemaVariables;
		private StreamProcessor outputStream;
		private String[] currentData;
		private Long currentTimeStamp;
		private String[] toSend;
		private int windowWidth=0;
		private int windowTime=0;
		@SuppressWarnings("unused")
		private int avgNumber=0;
		private long timeStartOffset=0;
		private int widthDone=0;
        private boolean sum_avgIndicator=false;
        private CEPLiteContext context;
        private HashMap<String,LinkedHashMap<String,String>> sum_avgOffsetStore;
		private LinkedHashMap<String,LinkedHashMap<String,String>> sum_avgStore=null;
		private ArrayList<String> sum_avgToSendOrder=null;
		
		public InputHandler(CEPLiteContext in_context,LinkedHashMap<String,String> args) throws UnknownHostException
		{
			queries=new ArrayList<Query>();
			schemaVariables=args;
			outputStream=null;
			currentData=null;
			currentTimeStamp=null;
			toSend=null;
			//client=new BasicCoapClient();
			this.context=in_context;
		}
		
				
		//adding queries to the particular source stream
		public void addQuery(String query) throws differentTypesException
		{
			if(query.contains("seq{"))
			{
				query=query.trim();
				queries.add(new continuousSequencePatternQuery(query,schemaVariables));
			}
			
			else if(query.contains("pat{"))
			{
				query=query.trim();
				queries.add(new sequenceInWindowPatternQuery(query, schemaVariables));
			} 
			//parsing complex predicates
		    else if(query.contains("and")||query.contains("or")||query.contains("not"))
			{
						query=query.trim();
						queries.add(new ComplexPredicateQuery(query));
			}
			
			//Checking equality
			else if(query.contains("=="))
			{			 
						//checking the parameters type
						query=query.trim();
						query=query.replaceAll("\\s+"," ");
						String[] parametersVars=query.split("==");
						int queryParameter=0;
						//explicit mentions of compare criteria
						if(  query.contains( "(")  &&  query.contains(")") )
						{
							
									queryParameter=1;
									//add all other data types
									if(schemaVariables.get(parametersVars[0]).contains("String"))
									{	
										queries.add(new EqualToBooleanQuery_String(query,queryParameter));
									}
									else if(schemaVariables.get(parametersVars[0]).contains("int"))
									{
										queries.add(new EqualToBooleanQuery_Int(query,queryParameter));
									}
									else if(schemaVariables.get(parametersVars[0]).contains("double"))
									{
										queries.add(new EqualToBooleanQuery_Double(query,queryParameter));
									}
									else if(schemaVariables.get(parametersVars[0]).contains("float"))
									{
										queries.add(new EqualToBooleanQuery_Float(query,queryParameter));
									}
							
						}	
						//existing fields
						else
						{
							
									if( schemaVariables.get(parametersVars[0]) == schemaVariables.get(parametersVars[1]) )
									{
										//add other types support later
										if(schemaVariables.get(parametersVars[0]).contains("String"))
										{	
											queries.add(new EqualToBooleanQuery_String(query,queryParameter));
										}
										else if(schemaVariables.get(parametersVars[0]).contains("int"))
										{
											queries.add(new EqualToBooleanQuery_Int(query,queryParameter));
										}
										else if(schemaVariables.get(parametersVars[0]).contains("double"))
										{
											queries.add(new EqualToBooleanQuery_Double(query,queryParameter));
										}
										else if(schemaVariables.get(parametersVars[0]).contains("float"))
										{
											queries.add(new EqualToBooleanQuery_Float(query,queryParameter));
										}
									
									}
									else
									{
										throw new differentTypesException("Wrong query(different types not comparable!)");
									}
						}
			}
			
			//checking not equal-to
			else if(query.contains("!="))
			{			 
						//checking the parameters type
						query=query.trim();
						query=query.replaceAll("\\s+"," ");
						String[] parametersVars=query.split("!=");
						int queryParameter=0;
						//explicit mentions of compare criteria
						if(  query.contains( "(")  &&  query.contains(")") )
						{
							
									queryParameter=1;
									//add all other data types
									if(schemaVariables.get(parametersVars[0]).contains("String"))
									{	
										queries.add(new notEqualToBooleanQuery_String(query,queryParameter));
									}
									else if(schemaVariables.get(parametersVars[0]).contains("int"))
									{
										queries.add(new notEqualToBooleanQuery_Int(query,queryParameter));
									}
									else if(schemaVariables.get(parametersVars[0]).contains("double"))
									{
										queries.add(new notEqualToBooleanQuery_Double(query,queryParameter));
									}
									else if(schemaVariables.get(parametersVars[0]).contains("float"))
									{
										queries.add(new notEqualToBooleanQuery_Float(query,queryParameter));
									}
							
						}	
						//existing fields
						else
						{
							
									if( schemaVariables.get(parametersVars[0]) == schemaVariables.get(parametersVars[1]) )
									{
										//add other types support later
										if(schemaVariables.get(parametersVars[0]).contains("String"))
										{	
											queries.add(new notEqualToBooleanQuery_String(query,queryParameter));
										}
										else if(schemaVariables.get(parametersVars[0]).contains("int"))
										{
											queries.add(new notEqualToBooleanQuery_Int(query,queryParameter));
										}
										else if(schemaVariables.get(parametersVars[0]).contains("double"))
										{
											queries.add(new notEqualToBooleanQuery_Double(query,queryParameter));
										}
										else if(schemaVariables.get(parametersVars[0]).contains("float"))
										{
											queries.add(new notEqualToBooleanQuery_Float(query,queryParameter));
										}
									
									}
									else
									{
										throw new differentTypesException("Wrong query(different types not comparable!)");
									}
						}
			}
			//checking more-than and less-than
			else if(query.contains(">") || query.contains("<")) 
			{			 
						//checking the parameters type
						query=query.trim();
						query=query.replaceAll("\\s+"," ");
						String[] parametersVars=null;
						if(query.contains(">"))
							parametersVars=query.split(">");
						else
							parametersVars=query.split("<");
						int queryParameter=0;
						//explicit mentions of compare criteria
						if(  query.contains( "(")  &&  query.contains(")") )
						{
							
									queryParameter=1;
									//add all other data types
									if(schemaVariables.get(parametersVars[0]).contains("int"))
									{
									    queries.add(new moreThanlessThanBooleanQuery_Int(query,queryParameter));
									}
									else if(schemaVariables.get(parametersVars[0]).contains("double"))
									{
										queries.add(new moreThanlessThanBooleanQuery_Double(query,queryParameter));
									}
									else if(schemaVariables.get(parametersVars[0]).contains("float"))
									{
										queries.add(new moreThanlessThanBooleanQuery_Float(query,queryParameter));
									}
							
						}	
						//existing fields
						else
						{
							
									if( schemaVariables.get(parametersVars[0]) == schemaVariables.get(parametersVars[1]) )
									{
										//add other types support later
										if(schemaVariables.get(parametersVars[0]).contains("int"))
										{
											queries.add(new moreThanlessThanBooleanQuery_Int(query,queryParameter));
										}
										else if(schemaVariables.get(parametersVars[0]).contains("double"))
										{
											queries.add(new moreThanlessThanBooleanQuery_Double(query,queryParameter));
										}
										else if(schemaVariables.get(parametersVars[0]).contains("float"))
										{
											queries.add(new moreThanlessThanBooleanQuery_Float(query,queryParameter));
										}
									
									}
									else
									{
										throw new differentTypesException("Wrong query(different types not comparable!)");
									}
						}
			}
			
			
		}
		
		public void clearQueries()
		{
			queries.clear();
		} 
		
		public void setOutputHandler(StreamProcessor streamOutputTo,String in_toSend)
		{
			this.outputStream=streamOutputTo;
			if(in_toSend.contains("avg(")||in_toSend.contains("sum(")||in_toSend.contains("max(")) 
			{
				sum_avgStore=new LinkedHashMap<String,LinkedHashMap<String,String>>();
				sum_avgOffsetStore=new HashMap<String,LinkedHashMap<String,String>>();
				sum_avgIndicator=true;
				sum_avgToSendOrder=new ArrayList<String>();
				String[] temp=null;
				if(in_toSend.contains("#window.length")){
					windowWidth=Integer.parseInt(in_toSend.split("#window.length")[1].replace("(","").replace(")",""));
					temp=in_toSend.split("#window.length")[0].split(",");
				}
				else if(in_toSend.contains("#window.time")){  
					windowTime=Integer.parseInt(in_toSend.split("#window.time")[1].replace("(","").replace(")",""));
					temp=in_toSend.split("#window.time")[0].split(","); 
				}
				for(String var:temp)
				{
					if(var.contains("sum("))
					{
						LinkedHashMap<String,String> temp_map=new LinkedHashMap<String,String>();
						
						if(var.contains("+") || var.contains("-")){
							var=var.replaceAll("\\s+","");
							String varname=var.substring(0,var.indexOf(")"));
							String offset=var.substring(var.indexOf(")")+1,var.length());
							temp_map.put("sum","0");
							if(sum_avgOffsetStore.containsKey(varname.replace("sum(","")))
							{
								sum_avgOffsetStore.get(varname.replace("sum(","")).put("sum",offset);								
							}
							else
							{
								LinkedHashMap<String,String> temp1=new LinkedHashMap<String,String>();
								temp1.put("sum",offset);
								sum_avgOffsetStore.put(varname.replace("sum(",""),temp1);
							}
							if(sum_avgStore.containsKey(varname.replace("sum(","")))
							{
								sum_avgStore.get(varname.replace("sum(","")).put("sum","0");	
							}	
							else
								sum_avgStore.put(varname.replace("sum(",""),temp_map);
							
							sum_avgToSendOrder.add(varname.replace("sum(","")+","+"sum");
						}
						else{
							temp_map.put("sum","0");
							if(sum_avgStore.containsKey(var.replace("sum(","").replace(")","")))
								sum_avgStore.get(var.replace("sum(","").replace(")","")).put("sum","0");
							else	
								sum_avgStore.put(var.replace("sum(","").replace(")",""),temp_map);
							
							sum_avgToSendOrder.add(var.replace("sum(","").replace(")","")+","+"sum");
						}
							
					}
					else if(var.contains("avg("))
					{
						LinkedHashMap<String,String> temp_map=new LinkedHashMap<String,String>();
						if(var.contains("+") || var.contains("-")){
							var=var.replaceAll("\\s+","");
							String varname=var.substring(0,var.indexOf(")"));
							String offset=var.substring(var.indexOf(")")+1,var.length());
							temp_map.put("avg","0");
							if(sum_avgOffsetStore.containsKey(varname.replace("avg(","")))
							{
								sum_avgOffsetStore.get(varname.replace("avg(","")).put("avg",offset);								
							}
							else
							{
								LinkedHashMap<String,String> temp1=new LinkedHashMap<String,String>();
								temp1.put("avg",offset);
								sum_avgOffsetStore.put(varname.replace("avg(",""),temp1);
							}
							if(sum_avgStore.containsKey(varname.replace("avg(","")))
							{
								sum_avgStore.get(varname.replace("avg(","")).put("avg","0");	
							}	
							else
								sum_avgStore.put(varname.replace("avg(",""),temp_map);
							
							sum_avgToSendOrder.add(varname.replace("avg(","")+","+"avg");
						}
						else{
							temp_map.put("avg","0");
							if(sum_avgStore.containsKey(var.replace("avg(","").replace(")","")))
								sum_avgStore.get(var.replace("avg(","").replace(")","")).put("avg","0");
							else	
								sum_avgStore.put(var.replace("avg(","").replace(")",""),temp_map);
							
							sum_avgToSendOrder.add(var.replace("avg(","").replace(")","")+","+"avg");
						
						}
					}
					else if(var.contains("max"))
					{
						LinkedHashMap<String,String> temp_map=new LinkedHashMap<String,String>();
						if(var.contains("+") || var.contains("-")){
							var=var.replaceAll("\\s+","");
							String varname=var.substring(0,var.indexOf(")"));
							String offset=var.substring(var.indexOf(")")+1,var.length());
							temp_map.put("max",null);
							if(sum_avgOffsetStore.containsKey(varname.replace("max(","")))
							{
								sum_avgOffsetStore.get(varname.replace("max(","")).put("max",offset);								
							}
							else
							{
								LinkedHashMap<String,String> temp1=new LinkedHashMap<String,String>();
								temp1.put("max",offset);
								sum_avgOffsetStore.put(varname.replace("max(",""),temp1);
							}
							if(sum_avgStore.containsKey(varname.replace("max(","")))
							{
								sum_avgStore.get(varname.replace("max(","")).put("max",null);	
							}	
							else
								sum_avgStore.put(varname.replace("max(",""),temp_map);
							
							sum_avgToSendOrder.add(varname.replace("max(","")+","+"max");
						}
						else{
							temp_map.put("max",null);
							if(sum_avgStore.containsKey(var.replace("max(","").replace(")","")))
								sum_avgStore.get(var.replace("max(","").replace(")","")).put("max",null);
							else	
								sum_avgStore.put(var.replace("max(","").replace(")",""),temp_map);
							
							sum_avgToSendOrder.add(var.replace("max(","").replace(")","")+","+"max");
						}
					}
					else
					{
						sum_avgStore.put(var,null);
					}
				}
				
			}
			else	
				toSend=in_toSend.split(",");
		}
		public void setOutputHandler(StreamProcessor streamOutputTo)
		{
			this.outputStream=streamOutputTo;
		}
		
		//change type to String[] from void  when unit testing
		public void send(TimeEventPair args) throws lessArgumentsException, differentTypesException, JSONException
		{
			currentData=null;
			currentTimeStamp=args.getTime();
			//System.out.println(currentTimeStamp); 
			//System.out.println(currentTimeStamp+windowTime); 
			String[] argu=args.getEvent();
			if(schemaVariables.size()!=argu.length)
			{
				
				throw new lessArgumentsException("Not enough/too many arguments!");
				 
			}
			else
			{	
				//currentData=Arrays.copyOf(argu, argu.length);
				int length=argu.length;
				currentData=new String[length];
				final int ZERO=0;
				System.arraycopy(argu, ZERO,currentData,ZERO,length);
				//currentData=new String[argu.length];
				/*for(int k=0;k<argu.length;k++)
				{
					currentData[k]=argu[k];
				}*/
				//parsing input data and adding it to the 
				HashMap<String,String> currentDataMap=new HashMap<String,String>();
				int i=0;
				for(Map.Entry<String,String> entry:schemaVariables.entrySet())
		        {
						currentDataMap.put(entry.getKey(),argu[i]);
						i++;
		        }
				
				//now we have a set of variables and their values....
				
				//send them to execute the queries
				//return the below when unit testing
				runQueries(currentDataMap);
			}
		}
		//change type to String[] from void  when unit testing
		private void runQueries(HashMap<String,String> currentDataMap) throws lessArgumentsException, differentTypesException, JSONException
		{	if(queries.size()==0)
			{			
						if(sum_avgIndicator==true)
						{			 
								if(windowWidth>0)
								{
												if(outputStream==null)
												{
													if(widthDone<=windowWidth)
													{
														for(Map.Entry<String,LinkedHashMap<String,String>> entry:sum_avgStore.entrySet())
														{	
															if(schemaVariables.get(entry.getKey()).contains("int"))
															{ 	
																if(entry.getValue().containsKey("sum"))
																{
																	entry.getValue().put("sum", ((Integer)(Integer.parseInt(entry.getValue().get("sum"))+Integer.parseInt(currentDataMap.get(entry.getKey())))).toString() );  
																}
																if(entry.getValue().containsKey("avg"))
																{
																	entry.getValue().put("avg", ((Integer)(Integer.parseInt(entry.getValue().get("avg"))+Integer.parseInt(currentDataMap.get(entry.getKey())))).toString() );  
																}
																if(entry.getValue().containsKey("max"))
																{
																	if(entry.getValue().get("max")==null)
																	{
																		entry.getValue().put("max",currentDataMap.get(entry.getKey())); 
																	}
																	else
																	{
																		if( Integer.parseInt(entry.getValue().get("max")) < Integer.parseInt(currentDataMap.get(entry.getKey())))
																		{
																			entry.getValue().put("max",currentDataMap.get(entry.getKey()));
																		}
																	}
																}
															}
														}
														widthDone++;
													}
													if(widthDone==windowWidth)
													{
														//output
														widthDone=0;
														JSONObject objectToSend=new JSONObject();
														//for(Map.Entry<String,LinkedHashMap<String,String>> entry:sum_avgStore.entrySet())
														for(String order:sum_avgToSendOrder)
														{
															String varname=order.split(",")[0];
															String operation=order.split(",")[1];
															
															if(operation.contains("sum")) 
															{
																if(sum_avgOffsetStore.containsKey(varname)){
																	if(sum_avgOffsetStore.get(varname).containsKey("sum")){ 
																		Integer a=(Integer.parseInt(sum_avgStore.get(varname).get("sum"))+Integer.parseInt(sum_avgOffsetStore.get(varname).get("sum")));
																		//Log.d("sum",a.toString());
																		objectToSend.put("sum("+varname+")",a.toString());
																		//System.out.print((Integer.parseInt(sum_avgStore.get(varname).get("sum"))+Integer.parseInt(sum_avgOffsetStore.get(varname).get("sum")))+" ");
																	}
																	else
																		//Log.d("sum",sum_avgStore.get(varname).get("sum"));
																		objectToSend.put("sum("+varname+")",sum_avgStore.get(varname).get("sum"));
																		//System.out.print(sum_avgStore.get(varname).get("sum")+" "); 
																}
																else 
																	objectToSend.put("sum("+varname+")",sum_avgStore.get(varname).get("sum"));
																	//Log.d("sum",sum_avgStore.get(varname).get("sum"));
																	//System.out.print(sum_avgStore.get(varname).get("sum")+" "); 
																sum_avgStore.get(varname).put("sum","0"); 
															}
															else if(operation.contains("avg")) 
															{
																if(sum_avgOffsetStore.containsKey(varname))
																{	
																	if(sum_avgOffsetStore.get(varname).containsKey("avg")){
																		Double a=(Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth+Integer.parseInt(sum_avgOffsetStore.get(varname).get("avg")));
																		//System.out.print((Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth+Integer.parseInt(sum_avgOffsetStore.get(varname).get("avg"))));
																		//Log.d("avg",a.toString());
																		objectToSend.put("avg("+varname+")",a.toString());
																	}
																	else
																	{
																		Double a=Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth;
																		//System.out.print(Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth);
																		//Log.d("avg",a.toString());
																		objectToSend.put("avg("+varname+")",a.toString());
																	}
																	//System.out.print(" ");
																}
																else{
																	Double a=Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth;
																	//Log.d("avg",a.toString());
																	objectToSend.put("avg("+varname+")",a.toString());
																	//System.out.print(Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth);
																	//System.out.print(" "); 
																}	
																sum_avgStore.get(varname).put("avg","0");
															}
															else if(operation.contains("max"))
															{
																if(sum_avgOffsetStore.containsKey(varname)){
																	if(sum_avgOffsetStore.get(varname).containsKey("max")){
																		Integer a=(Integer.parseInt(sum_avgStore.get(varname).get("max"))+Integer.parseInt(sum_avgOffsetStore.get(varname).get("max")));
																		//Log.d("max",a.toString());
																		objectToSend.put("max("+varname+")",a.toString());
																		//System.out.print((Integer.parseInt(sum_avgStore.get(varname).get("max"))+Integer.parseInt(sum_avgOffsetStore.get(varname).get("max")))+" ");
																	}
																	else
																		objectToSend.put("max("+varname+")",sum_avgStore.get(varname).get("max"));
																		//Log.d("max",sum_avgStore.get(varname).get("max"));
																		//System.out.print(sum_avgStore.get(varname).get("max")+" ");
																}
																else
																	objectToSend.put("max("+varname+")",sum_avgStore.get(varname).get("max"));
																	//Log.d("max",sum_avgStore.get(varname).get("max"));
																	//System.out.print(sum_avgStore.get(varname).get("max")+" "); 
																sum_avgStore.get(varname).put("max",null);
															}
														}
														objectToSend.put("TimeStamp","null");
								
														//client.sendMessage(objectToSend.toString());
														context.addToOutputQueue(objectToSend);
														//System.out.println(" ");
														
													}
													
												}
									}
									else if(windowTime>0)
									{
										     	if(outputStream==null)
												{
													if(timeStartOffset==0)
													{
															timeStartOffset=currentTimeStamp;
															 
													}
													if(timeStartOffset+windowTime<currentTimeStamp)
													{
														JSONObject objectToSend=new JSONObject();
														for(String order:sum_avgToSendOrder)
														{
															String varname=order.split(",")[0];
															String operation=order.split(",")[1];
															
															if(operation.contains("sum")) 
															{
																if(sum_avgOffsetStore.containsKey(varname)){
																	if(sum_avgOffsetStore.get(varname).containsKey("sum")){ 
																		Integer a=(Integer.parseInt(sum_avgStore.get(varname).get("sum"))+Integer.parseInt(sum_avgOffsetStore.get(varname).get("sum")));
																		objectToSend.put("sum("+varname+")",a.toString());
																		//Log.d("sum",a.toString());
																		//System.out.print((Integer.parseInt(sum_avgStore.get(varname).get("sum"))+Integer.parseInt(sum_avgOffsetStore.get(varname).get("sum")))+" ");
																	}
																	else
																		objectToSend.put("sum("+varname+")",sum_avgStore.get(varname).get("sum"));
																		//Log.d("sum",sum_avgStore.get(varname).get("sum"));
																		//System.out.print(sum_avgStore.get(varname).get("sum")+" "); 
																}
																else 
																	objectToSend.put("sum("+varname+")",sum_avgStore.get(varname).get("sum"));
																	//Log.d("sum",sum_avgStore.get(varname).get("sum"));
																	//System.out.print(sum_avgStore.get(varname).get("sum")+" "); 
																sum_avgStore.get(varname).put("sum","0"); 
															}
															else if(operation.contains("avg")) 
															{
																if(sum_avgOffsetStore.containsKey(varname))
																{	
																	if(sum_avgOffsetStore.get(varname).containsKey("avg")){
																		Double a=(Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth+Integer.parseInt(sum_avgOffsetStore.get(varname).get("avg")));
																		//System.out.print((Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth+Integer.parseInt(sum_avgOffsetStore.get(varname).get("avg"))));
																		//Log.d("avg",a.toString());
																		objectToSend.put("avg("+varname+")",a.toString());
																	}
																	else
																	{
																		Double a=Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth;
																		//System.out.print(Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth);
																		//Log.d("avg",a.toString());
																		objectToSend.put("avg("+varname+")",a.toString());
																	}
																	//System.out.print(" ");
																}
																else{
																	Double a=Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth;
																	
																	objectToSend.put("avg("+varname+")",a.toString());
																	//System.out.print(Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth);
																	//System.out.print(" "); 
																}	
																sum_avgStore.get(varname).put("avg","0");
															}
															else if(operation.contains("max"))
															{
																if(sum_avgOffsetStore.containsKey(varname)){
																	if(sum_avgOffsetStore.get(varname).containsKey("max")){
																		Integer a=(Integer.parseInt(sum_avgStore.get(varname).get("max"))+Integer.parseInt(sum_avgOffsetStore.get(varname).get("max")));
																		//Log.d("max",a.toString());
																		objectToSend.put("max("+varname+")",a.toString());
																		//System.out.print((Integer.parseInt(sum_avgStore.get(varname).get("max"))+Integer.parseInt(sum_avgOffsetStore.get(varname).get("max")))+" ");
																	}
																	else
																		objectToSend.put("max("+varname+")",sum_avgStore.get(varname).get("max"));
																		//Log.d("max",sum_avgStore.get(varname).get("max"));
																		//System.out.print(sum_avgStore.get(varname).get("max")+" ");
																}
																else
																	objectToSend.put("max("+varname+")",sum_avgStore.get(varname).get("max"));
																	//Log.d("max",sum_avgStore.get(varname).get("max"));
																	//System.out.print(sum_avgStore.get(varname).get("max")+" "); 
																sum_avgStore.get(varname).put("max",null);
															}
														}
														objectToSend.put("TimeStamp","null");
														
														//client.sendMessage(objectToSend.toString());
														context.addToOutputQueue(objectToSend);
														//System.out.println(" ");
														timeStartOffset=currentTimeStamp;
														avgNumber=0;
													}
													if(timeStartOffset+windowTime>currentTimeStamp)
													{
														for(Map.Entry<String,LinkedHashMap<String,String>> entry:sum_avgStore.entrySet())
														{
															if(schemaVariables.get(entry.getKey()).contains("int"))
															{ 	
																if(entry.getValue().containsKey("sum"))
																{
																	entry.getValue().put("sum", ((Integer)(Integer.parseInt(entry.getValue().get("sum"))+Integer.parseInt(currentDataMap.get(entry.getKey())))).toString() );  
																}
																if(entry.getValue().containsKey("avg"))
																{
																	entry.getValue().put("avg", ((Integer)(Integer.parseInt(entry.getValue().get("avg"))+Integer.parseInt(currentDataMap.get(entry.getKey())))).toString() );  
																	avgNumber++;
																}
																if(entry.getValue().containsKey("max"))
																{
																	if(entry.getValue().get("max")==null)
																	{
																		entry.getValue().put("max",currentDataMap.get(entry.getKey())); 
																	}
																	else
																	{
																		if( Integer.parseInt(entry.getValue().get("max")) < Integer.parseInt(currentDataMap.get(entry.getKey())))
																		{
																			entry.getValue().put("max",currentDataMap.get(entry.getKey()));
																		}
																	}
																}
															}
														}
													}
													
												}
									}
						}
						else if(outputStream!=null)
						{
							if(toSend!=null)
							 {
								 ArrayList<String> list_temp=new ArrayList<String>();
								 for(String varname:toSend)
								 {
									 list_temp.add(currentDataMap.get(varname));
								 }
								 //System.out.println(list_temp.toString());
								 String[] temp_string=new String[toSend.length];
								 context.pollingQueues.get(outputStream.streamId).offer(new TimeEventPair(currentTimeStamp, list_temp.toArray(temp_string))); 
								 //System.out.println(context.pollingQueues.get(outputStream.streamId).toString()); 
							 }
						}
						else if(outputStream==null)
						{	JSONObject objectToSend=new JSONObject();	
							  if(toSend!=null)
							  {
								  for(String varname:toSend)
								  {
									  //System.out.print(currentDataMap.get(varname)+" ");  
									  //Log.d("data",currentDataMap.get(varname));
									  objectToSend.put(varname,currentDataMap.get(varname));
								  }
								  objectToSend.put("TimeStamp",currentTimeStamp.toString());
								  context.addToOutputQueue(objectToSend);
								  //System.out.println(); 
							  }
						}
			}
			else if(queries.size()>0)
			{
			for(Query query:queries)
			{
					//cheking if query is Pattern-Query
					boolean variable=true;
					boolean variable1=false;
					ArrayList<TimeEventPair> matched_Events=new ArrayList<TimeEventPair>();
					if(PatternQuery.class.isAssignableFrom(query.getClass()))
					{
						variable1=true;
						if(query instanceof continuousSequencePatternQuery)
						{
							matched_Events=((continuousSequencePatternQuery) query).process(currentTimeStamp,currentData,currentDataMap,schemaVariables);
						}
						else if(query instanceof sequenceInWindowPatternQuery)
						{
							matched_Events=((sequenceInWindowPatternQuery) query).process(currentTimeStamp,currentData,currentDataMap,schemaVariables);
						}
					}
					//checking if the query is a Boolean-Query
					
					if(BooleanQuery.class.isAssignableFrom(query.getClass()))
					{
						if(query instanceof EqualToBooleanQuery_String)
						{
							variable=((EqualToBooleanQuery_String) query).process(currentDataMap);
						}
						else if(query instanceof notEqualToBooleanQuery_String)
						{
							variable=((notEqualToBooleanQuery_String) query).process(currentDataMap);
						}
						else if(query instanceof notEqualToBooleanQuery_Int)
						{
							variable=((notEqualToBooleanQuery_Int) query).process(currentDataMap);
						}
						else if(query instanceof EqualToBooleanQuery_Int)
						{
							variable=((EqualToBooleanQuery_Int) query).process(currentDataMap);
						}
						else if(query instanceof moreThanlessThanBooleanQuery_Int)
						{
							variable=((moreThanlessThanBooleanQuery_Int) query).process(currentDataMap);
						}
						else if(query instanceof EqualToBooleanQuery_Float)
						{
							variable=((EqualToBooleanQuery_Float) query).process(currentDataMap);
						}
						else if(query instanceof notEqualToBooleanQuery_Float)
						{
							variable=((notEqualToBooleanQuery_Float) query).process(currentDataMap);
						}
						else if(query instanceof moreThanlessThanBooleanQuery_Float)
						{
							variable=((moreThanlessThanBooleanQuery_Float) query).process(currentDataMap);
						}
						else if(query instanceof EqualToBooleanQuery_Double)
						{
							variable=((EqualToBooleanQuery_Double) query).process(currentDataMap);
						}
						else if(query instanceof notEqualToBooleanQuery_Double)
						{
							variable=((notEqualToBooleanQuery_Double) query).process(currentDataMap);
						}
						else if(query instanceof moreThanlessThanBooleanQuery_Double)
						{
							variable=((moreThanlessThanBooleanQuery_Double) query).process(currentDataMap);
						}
						else if(query instanceof ComplexPredicateQuery)
						{
							variable=((ComplexPredicateQuery) query).process(currentDataMap, schemaVariables);
							
						}
						
					}
					
				
					//if the data predicate passes the query hand it to the output handler
					if(variable && !variable1)
					{	
						
						if(sum_avgIndicator==false)
						{	
									 if(outputStream==null)
									 {
										 //remove comment from below for unit testing
										 //return currentData;
										 
										 //comment for junit testing
										 /*for(String arg:currentData)
										 {
											 //System.out.println(arg+" ");
											 Log.d("data",arg);
										 }*/ 
										 JSONObject objectToSend=new JSONObject();
										 for(Entry<String,String> entry:currentDataMap.entrySet())
										 {
											 objectToSend.put(entry.getKey(),entry.getValue());
										 }
										 objectToSend.put("TimeStamp",currentTimeStamp.toString());
										 
										 //client.sendMessage(objectToSend.toString());
										 context.addToOutputQueue(objectToSend);
									 }
									 else
									 {
										 //return the below for unit testing
										 if(toSend!=null)
										 {
											 ArrayList<String> list_temp=new ArrayList<String>();
											 for(String varname:toSend)
											 {
												 list_temp.add(currentDataMap.get(varname));
											 }
											 //System.out.println(list_temp.toString());
											 String[] temp_string=new String[toSend.length];
											 context.pollingQueues.get(outputStream.streamId).offer(new TimeEventPair(currentTimeStamp, list_temp.toArray(temp_string))); 
											 //System.out.println(context.pollingQueues.get(outputStream.streamId).toString()); 
										 }
										 else
										 {
											 context.pollingQueues.get(outputStream.streamId).offer(new TimeEventPair(currentTimeStamp,currentData)); 
										 }
									 }
						}
						else
						{			 
								if(windowWidth>0)
								{	
															if(outputStream==null)
															{
																if(widthDone<=windowWidth)
																{
																	for(Map.Entry<String,LinkedHashMap<String,String>> entry:sum_avgStore.entrySet())
																	{
																		if(schemaVariables.get(entry.getKey()).contains("int"))
																		{ 	
																			if(entry.getValue().containsKey("sum"))
																			{
																				entry.getValue().put("sum", ((Integer)(Integer.parseInt(entry.getValue().get("sum"))+Integer.parseInt(currentDataMap.get(entry.getKey())))).toString() );  
																			}
																			else if(entry.getValue().containsKey("avg"))
																			{
																				entry.getValue().put("avg", ((Integer)(Integer.parseInt(entry.getValue().get("avg"))+Integer.parseInt(currentDataMap.get(entry.getKey())))).toString() );  
																			}
																			else if(entry.getValue().containsKey("max"))
																			{
																				if(entry.getValue().get("max")==null)
																				{
																					entry.getValue().put("max",currentDataMap.get(entry.getKey())); 
																				}
																				else
																				{
																					if( Integer.parseInt(entry.getValue().get("max")) < Integer.parseInt(currentDataMap.get(entry.getKey())))
																					{
																						entry.getValue().put("max",currentDataMap.get(entry.getKey()));
																					}
																				}
																			}
																		}
																	}
																	widthDone++;
																}
																if(widthDone==windowWidth)
																{
																	//output
																	widthDone=0;
																	JSONObject objectToSend=new JSONObject();
																	for(String order:sum_avgToSendOrder)
																	{
																		String varname=order.split(",")[0];
																		String operation=order.split(",")[1];
																		
																		if(operation.contains("sum")) 
																		{
																			if(sum_avgOffsetStore.containsKey(varname)){
																				if(sum_avgOffsetStore.get(varname).containsKey("sum")){ 
																					Integer a=(Integer.parseInt(sum_avgStore.get(varname).get("sum"))+Integer.parseInt(sum_avgOffsetStore.get(varname).get("sum")));
																					objectToSend.put("sum("+varname+")",a.toString());
																					//Log.d("sum",a.toString());
																					//System.out.print((Integer.parseInt(sum_avgStore.get(varname).get("sum"))+Integer.parseInt(sum_avgOffsetStore.get(varname).get("sum")))+" ");
																				}
																				else
																					//Log.d("sum",sum_avgStore.get(varname).get("sum"));
																					objectToSend.put("sum("+varname+")",sum_avgStore.get(varname).get("sum"));
																					//System.out.print(sum_avgStore.get(varname).get("sum")+" "); 
																			}
																			else 
																				objectToSend.put("sum("+varname+")",sum_avgStore.get(varname).get("sum"));
																				//Log.d("sum",sum_avgStore.get(varname).get("sum"));
																				//System.out.print(sum_avgStore.get(varname).get("sum")+" "); 
																			sum_avgStore.get(varname).put("sum","0"); 
																		}
																		else if(operation.contains("avg")) 
																		{
																			if(sum_avgOffsetStore.containsKey(varname))
																			{	
																				if(sum_avgOffsetStore.get(varname).containsKey("avg")){
																					Double a=(Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth+Integer.parseInt(sum_avgOffsetStore.get(varname).get("avg")));
																					objectToSend.put("avg("+varname+")",a.toString());
																					//System.out.print((Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth+Integer.parseInt(sum_avgOffsetStore.get(varname).get("avg"))));
																					//Log.d("avg",a.toString());
																				}
																				else
																				{
																					Double a=Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth;
																					objectToSend.put("avg("+varname+")",a.toString());
																					//System.out.print(Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth);
																					//Log.d("avg",a.toString());
																				}
																				//System.out.print(" ");
																			}
																			else{
																				Double a=Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth;
																				objectToSend.put("avg("+varname+")",a.toString());
																				//Log.d("avg",a.toString());
																				//System.out.print(Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth);
																				//System.out.print(" "); 
																			}	
																			sum_avgStore.get(varname).put("avg","0");
																		}
																		else if(operation.contains("max"))
																		{
																			if(sum_avgOffsetStore.containsKey(varname)){
																				if(sum_avgOffsetStore.get(varname).containsKey("max")){
																					Integer a=(Integer.parseInt(sum_avgStore.get(varname).get("max"))+Integer.parseInt(sum_avgOffsetStore.get(varname).get("max")));
																					objectToSend.put("max("+varname+")",a.toString());
																					//Log.d("max",a.toString());
																					//System.out.print((Integer.parseInt(sum_avgStore.get(varname).get("max"))+Integer.parseInt(sum_avgOffsetStore.get(varname).get("max")))+" ");
																				}
																				else
																					objectToSend.put("max("+varname+")",sum_avgStore.get(varname).get("max"));
																					//Log.d("max",sum_avgStore.get(varname).get("max"));
																					//System.out.print(sum_avgStore.get(varname).get("max")+" ");
																			}
																			else
																				objectToSend.put("max("+varname+")",sum_avgStore.get(varname).get("max"));
																				//Log.d("max",sum_avgStore.get(varname).get("max"));
																				//System.out.print(sum_avgStore.get(varname).get("max")+" "); 
																			sum_avgStore.get(varname).put("max",null);
																		}
																	}
																	objectToSend.put("TimeStamp","null");
																	
																	//client.sendMessage(objectToSend.toString());
																	context.addToOutputQueue(objectToSend);
																	//System.out.println(" ");
																}
																
															}
								}
								else if(windowTime>0)
								{
											if(outputStream==null)
											{
												if(timeStartOffset==0)
												{
													timeStartOffset=currentTimeStamp;
												}
												if(timeStartOffset+windowTime<currentTimeStamp)
												{
													JSONObject objectToSend=new JSONObject();
													for(String order:sum_avgToSendOrder)
													{
														String varname=order.split(",")[0];
														String operation=order.split(",")[1];
														
														if(operation.contains("sum")) 
														{
															if(sum_avgOffsetStore.containsKey(varname)){
																if(sum_avgOffsetStore.get(varname).containsKey("sum")){ 
																	Integer a=(Integer.parseInt(sum_avgStore.get(varname).get("sum"))+Integer.parseInt(sum_avgOffsetStore.get(varname).get("sum")));
																	objectToSend.put("sum("+varname+")",a.toString());
																	//Log.d("sum",a.toString());
																	//System.out.print((Integer.parseInt(sum_avgStore.get(varname).get("sum"))+Integer.parseInt(sum_avgOffsetStore.get(varname).get("sum")))+" ");
																}
																else
																	//Log.d("sum",sum_avgStore.get(varname).get("sum"));
																	objectToSend.put("sum("+varname+")",sum_avgStore.get(varname).get("sum"));
																	//System.out.print(sum_avgStore.get(varname).get("sum")+" "); 
															}
															else 
																objectToSend.put("sum("+varname+")",sum_avgStore.get(varname).get("sum"));
																//Log.d("sum",sum_avgStore.get(varname).get("sum"));
																//System.out.print(sum_avgStore.get(varname).get("sum")+" "); 
															sum_avgStore.get(varname).put("sum","0"); 
														}
														else if(operation.contains("avg")) 
														{
															if(sum_avgOffsetStore.containsKey(varname))
															{	
																if(sum_avgOffsetStore.get(varname).containsKey("avg")){
																	Double a=(Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth+Integer.parseInt(sum_avgOffsetStore.get(varname).get("avg")));
																	objectToSend.put("avg("+varname+")",a.toString());
																	//System.out.print((Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth+Integer.parseInt(sum_avgOffsetStore.get(varname).get("avg"))));
																	//Log.d("avg",a.toString());
																}
																else
																{
																	Double a=Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth;
																	objectToSend.put("avg("+varname+")",a.toString());
																	//System.out.print(Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth);
																	//Log.d("avg",a.toString());
																}
																//System.out.print(" ");
															}
															else{
																Double a=Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth;
																objectToSend.put("avg("+varname+")",a.toString());
																//Log.d("avg",a.toString());
																//System.out.print(Double.parseDouble(sum_avgStore.get(varname).get("avg"))/windowWidth);
																//System.out.print(" "); 
															}	
															sum_avgStore.get(varname).put("avg","0");
														}
														else if(operation.contains("max"))
														{
															if(sum_avgOffsetStore.containsKey(varname)){
																if(sum_avgOffsetStore.get(varname).containsKey("max")){
																	Integer a=(Integer.parseInt(sum_avgStore.get(varname).get("max"))+Integer.parseInt(sum_avgOffsetStore.get(varname).get("max")));
																	objectToSend.put("max("+varname+")",a.toString());
																	//Log.d("max",a.toString());
																	//System.out.print((Integer.parseInt(sum_avgStore.get(varname).get("max"))+Integer.parseInt(sum_avgOffsetStore.get(varname).get("max")))+" ");
																}
																else
																	objectToSend.put("max("+varname+")",sum_avgStore.get(varname).get("max"));
																	//Log.d("max",sum_avgStore.get(varname).get("max"));
																	//System.out.print(sum_avgStore.get(varname).get("max")+" ");
															}
															else
																objectToSend.put("max("+varname+")",sum_avgStore.get(varname).get("max"));
																//Log.d("max",sum_avgStore.get(varname).get("max"));
																//System.out.print(sum_avgStore.get(varname).get("max")+" "); 
															sum_avgStore.get(varname).put("max",null);
														}
													}
													objectToSend.put("TimeStamp","null");
													
													//client.sendMessage(objectToSend.toString());
													context.addToOutputQueue(objectToSend);
													//System.out.println(" ");
													timeStartOffset=currentTimeStamp;
                                                    avgNumber=0;
												}
												if(timeStartOffset+windowTime>currentTimeStamp)
												{
													for(Map.Entry<String,LinkedHashMap<String,String>> entry:sum_avgStore.entrySet())
													{
														if(schemaVariables.get(entry.getKey()).contains("int"))
														{ 	
															if(entry.getValue().containsKey("sum"))
															{
																entry.getValue().put("sum", ((Integer)(Integer.parseInt(entry.getValue().get("sum"))+Integer.parseInt(currentDataMap.get(entry.getKey())))).toString() );  
															}
															else if(entry.getValue().containsKey("avg"))
															{
																entry.getValue().put("avg", ((Integer)(Integer.parseInt(entry.getValue().get("avg"))+Integer.parseInt(currentDataMap.get(entry.getKey())))).toString() );  
																avgNumber++;
															}
															else if(entry.getValue().containsKey("max"))
															{
																if(entry.getValue().get("max")==null)
																{
																	entry.getValue().put("max",currentDataMap.get(entry.getKey())); 
																}
																else
																{
																	if( Integer.parseInt(entry.getValue().get("max")) < Integer.parseInt(currentDataMap.get(entry.getKey())))
																	{
																		entry.getValue().put("max",currentDataMap.get(entry.getKey()));
																	}
																}
															}
														}
													}
												}
												
											}
								}
									
						}
					}
					if(matched_Events.size()!=0)
					{	
						if(outputStream==null)
						{
							JSONObject objectToSend=new JSONObject();
							Integer a=0;
						    for(TimeEventPair event:matched_Events)
							{
						    	a++;
						    	String[] str=event.getEvent();
						    	JSONObject objectToSendTemp=new JSONObject();
						    	objectToSendTemp.put("TimeStamp",event.getTime().toString());
						    	Iterator<String> it=schemaVariables.keySet().iterator();
						    	for(String var:str)
								{
									//System.out.print(var+" ");
									//Log.d("data",var);
						    		objectToSendTemp.put(it.next(),var);
						    	}
						    	objectToSend.put(a.toString(),objectToSendTemp);
								//System.out.println(""); 
							}
						    
						    //client.sendMessage(objectToSend.toString());
						    context.addToOutputQueue(objectToSend);
						}
						else
						{
							if(toSend!=null)
							{
								for(TimeEventPair event:matched_Events)
								{	
									String[] str=event.getEvent();
									ArrayList<String> list_temp=new ArrayList<String>();
									for(String var:toSend)
									{
										int g=0;
										for(Map.Entry<String,String> entry:schemaVariables.entrySet())
										{
											if(entry.getKey().equals(var)) 
											{
												list_temp.add(str[g]);
												break;
											}
											else
											{
												g++;
											}
										}
									}
									String[] temp_string=new String[toSend.length];
									context.pollingQueues.get(outputStream.streamId).offer(new TimeEventPair(event.getTime(), list_temp.toArray(temp_string))   );
								}
							}
							else
							{
								for(TimeEventPair data:matched_Events)
								{ 
									context.pollingQueues.get(outputStream.streamId).offer(data);
								}
							}
						}
					}
					
				
			}
			}
			//add the below for unit testing
			//return currentData;
			 
			 
		}
		
}
