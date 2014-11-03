package cep.query.patternQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import cep.core.TimeEventPair;
import cep.core.differentTypesException;
import cep.query.Query;
import cep.query.booleanQuery.BooleanQuery;
import cep.query.booleanQuery.EqualToBooleanQuery_Double;
import cep.query.booleanQuery.EqualToBooleanQuery_Float;
import cep.query.booleanQuery.EqualToBooleanQuery_Int;
import cep.query.booleanQuery.EqualToBooleanQuery_Long;
import cep.query.booleanQuery.EqualToBooleanQuery_String;
import cep.query.booleanQuery.moreThanlessThanBooleanQuery_Double;
import cep.query.booleanQuery.moreThanlessThanBooleanQuery_Float;
import cep.query.booleanQuery.moreThanlessThanBooleanQuery_Int;
import cep.query.booleanQuery.moreThanlessThanBooleanQuery_Long;
import cep.query.booleanQuery.notEqualToBooleanQuery_Double;
import cep.query.booleanQuery.notEqualToBooleanQuery_Float;
import cep.query.booleanQuery.notEqualToBooleanQuery_Int;
import cep.query.booleanQuery.notEqualToBooleanQuery_String;
import cep.query.booleanQuery.notEqualToBooleanQuery_Long;

public class continuousSequencePatternQuery extends PatternQuery{
		private int windowWidth=0;
		private int windowTime=0;
		private long timeStartOffset=0;
		private boolean boolean_holder=false;
		private LinkedHashMap<String,String> schemaVariables;
		private ArrayList<Query> queries=new ArrayList<Query>();
		private ArrayList<TimeEventPair> matchedEvents=new ArrayList<TimeEventPair>();
		private int patternMatch=0;
		private int patternDone=0;
		public continuousSequencePatternQuery(String in_query,LinkedHashMap<String,String> in_schemaVariables) throws differentTypesException
		{
			super(in_query);
			schemaVariables=in_schemaVariables;
			if(query.contains("#window.length"))
			{
				String temp=query.split("#window.length")[1];
				query=query.split("#window.length")[0];
				temp=temp.replace("(","").replace(")","");
				windowWidth=Integer.parseInt(temp);
			}
			else if(query.contains("#window.time"))
			{
				String temp=query.split("#window.time")[1];
				query=query.split("#window.time")[0];
				temp=temp.replace("(","").replace(")","");
				windowTime=Integer.parseInt(temp);
			}
			String currentQuery=query.replace("seq{","").replace("}","");
			//considering the patterns also as matching boolean-queries
			//boolean_holder=new boolean[currentQuery.split(",").length];
			for(String var:currentQuery.split(","))
			{
				if(var.contains("=="))
				{			 
							//checking the parameters type
							var=var.trim();
							var=var.replaceAll("\\s+"," ");
							String[] parametersVars=var.split("==");
							int queryParameter=0;
							//explicit mentions of compare criteria
							if(  var.contains( "(")  &&  var.contains(")") )
							{
								
										queryParameter=1;
										//add all other data types
										if(schemaVariables.get(parametersVars[0]).contains("String"))
										{	
											queries.add(new EqualToBooleanQuery_String(var,queryParameter));
										}
										else if(schemaVariables.get(parametersVars[0]).contains("int"))
										{
											queries.add(new EqualToBooleanQuery_Int(var,queryParameter));
										}
										else if(schemaVariables.get(parametersVars[0]).contains("double"))
										{
											queries.add(new EqualToBooleanQuery_Double(var,queryParameter));
										}
										else if(schemaVariables.get(parametersVars[0]).contains("float"))
										{
											queries.add(new EqualToBooleanQuery_Float(var,queryParameter));
										}
										else if(schemaVariables.get(parametersVars[0]).contains("long"))
										{
											queries.add(new EqualToBooleanQuery_Long(var,queryParameter));
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
												queries.add(new EqualToBooleanQuery_String(var,queryParameter));
											}
											else if(schemaVariables.get(parametersVars[0]).contains("int"))
											{
												queries.add(new EqualToBooleanQuery_Int(var,queryParameter));
											}
											else if(schemaVariables.get(parametersVars[0]).contains("double"))
											{
												queries.add(new EqualToBooleanQuery_Double(var,queryParameter));
											}
											else if(schemaVariables.get(parametersVars[0]).contains("float"))
											{
												queries.add(new EqualToBooleanQuery_Float(var,queryParameter));
											}
											else if(schemaVariables.get(parametersVars[0]).contains("long"))
											{
												queries.add(new EqualToBooleanQuery_Long(var,queryParameter));
											}
										
										}
										else
										{
											throw new differentTypesException("Wrong query(different types not comparable!)");
										}
							}
				}
				else if(var.contains("!="))
				{			 
							//checking the parameters type
							var=var.trim();
							var=var.replaceAll("\\s+"," ");
							String[] parametersVars=var.split("!=");
							int queryParameter=0;
							//explicit mentions of compare criteria
							if(  var.contains( "(")  &&  var.contains(")") )
							{
								
										queryParameter=1;
										//add all other data types
										if(schemaVariables.get(parametersVars[0]).contains("String"))
										{	
											queries.add(new notEqualToBooleanQuery_String(var,queryParameter));
										}
										else if(schemaVariables.get(parametersVars[0]).contains("int"))
										{
											queries.add(new notEqualToBooleanQuery_Int(var,queryParameter));
										}
										else if(schemaVariables.get(parametersVars[0]).contains("double"))
										{
											queries.add(new notEqualToBooleanQuery_Double(var,queryParameter));
										}
										else if(schemaVariables.get(parametersVars[0]).contains("float"))
										{
											queries.add(new notEqualToBooleanQuery_Float(var,queryParameter));
										}
										else if(schemaVariables.get(parametersVars[0]).contains("long"))
										{
											queries.add(new notEqualToBooleanQuery_Long(var,queryParameter));
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
												queries.add(new notEqualToBooleanQuery_String(var,queryParameter));
											}
											else if(schemaVariables.get(parametersVars[0]).contains("int"))
											{
												queries.add(new notEqualToBooleanQuery_Int(var,queryParameter));
											}
											else if(schemaVariables.get(parametersVars[0]).contains("double"))
											{
												queries.add(new notEqualToBooleanQuery_Double(var,queryParameter));
											}
											else if(schemaVariables.get(parametersVars[0]).contains("float"))
											{
												queries.add(new notEqualToBooleanQuery_Float(var,queryParameter));
											}
											else if(schemaVariables.get(parametersVars[0]).contains("long"))
											{
												queries.add(new notEqualToBooleanQuery_Long(var,queryParameter));
											}
										
										}
										else
										{
											throw new differentTypesException("Wrong query(different types not comparable!)");
										}
							}
				}
				else if(var.contains(">") || var.contains("<")) 
				{			 
							//checking the parameters type
							var=var.trim();
							var=var.replaceAll("\\s+"," ");
							String[] parametersVars=null;
							if(var.contains(">"))
								parametersVars=var.split(">");
							else
								parametersVars=var.split("<");
							int queryParameter=0;
							//explicit mentions of compare criteria
							if(  var.contains( "(")  &&  var.contains(")") )
							{
								
										queryParameter=1;
										//add all other data types
										if(schemaVariables.get(parametersVars[0]).contains("int"))
										{
										    queries.add(new moreThanlessThanBooleanQuery_Int(var,queryParameter));
										}
										else if(schemaVariables.get(parametersVars[0]).contains("double"))
										{
											queries.add(new moreThanlessThanBooleanQuery_Double(var,queryParameter));
										}
										else if(schemaVariables.get(parametersVars[0]).contains("float"))
										{
											queries.add(new moreThanlessThanBooleanQuery_Float(var,queryParameter));
										}
										else if(schemaVariables.get(parametersVars[0]).contains("long"))
										{
											queries.add(new moreThanlessThanBooleanQuery_Long(var,queryParameter));
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
												queries.add(new moreThanlessThanBooleanQuery_Int(var,queryParameter));
											}
											else if(schemaVariables.get(parametersVars[0]).contains("double"))
											{
												queries.add(new moreThanlessThanBooleanQuery_Double(var,queryParameter));
											}
											else if(schemaVariables.get(parametersVars[0]).contains("float"))
											{
												queries.add(new moreThanlessThanBooleanQuery_Float(var,queryParameter));
											}
											else if(schemaVariables.get(parametersVars[0]).contains("long"))
											{
												queries.add(new moreThanlessThanBooleanQuery_Long(var,queryParameter));
											}
										
										}
										else
										{
											throw new differentTypesException("Wrong query(different types not comparable!)");
										}
							}
				}
				
			}
			
		}

		@Override
		public ArrayList<TimeEventPair> process(HashMap<String, String> currentDataMap) {
			    ArrayList<TimeEventPair> NO_LIST=new ArrayList<TimeEventPair>(0);
				return NO_LIST;
		}
		public ArrayList<TimeEventPair> process(Long currentTimestamp,String[] currentData,HashMap<String,String> currentDataMap,LinkedHashMap<String,String> schemaVariables)
		{
									
			if(windowWidth>0)
			{	
									if(windowWidth-patternMatch < queries.size()-patternDone)
									{
										patternMatch++;
										boolean_holder=false;
										matchedEvents.clear();
										if(patternMatch>windowWidth)
										{
											patternMatch=0;
											patternDone=0;
										}
									}
									if(windowWidth-patternMatch >= queries.size()-patternDone)
									{
													if(matchedEvents.size()>=queries.size())
													{
														matchedEvents.clear();
													}
													patternMatch++;
													boolean variable=false;
													if(BooleanQuery.class.isAssignableFrom(queries.get(patternDone).getClass()))
													{
														if(queries.get(patternDone) instanceof EqualToBooleanQuery_String)
														{
															variable=((EqualToBooleanQuery_String) queries.get(patternDone)).process(currentDataMap);
														}
														else if(queries.get(patternDone) instanceof notEqualToBooleanQuery_String)
														{
															variable=((notEqualToBooleanQuery_String) queries.get(patternDone)).process(currentDataMap);
														}
														else if(queries.get(patternDone) instanceof notEqualToBooleanQuery_Int)
														{
															variable=((notEqualToBooleanQuery_Int) queries.get(patternDone)).process(currentDataMap);
														}
														else if(queries.get(patternDone) instanceof EqualToBooleanQuery_Int)
														{
															variable=((EqualToBooleanQuery_Int) queries.get(patternDone)).process(currentDataMap);
														}
														else if(queries.get(patternDone) instanceof moreThanlessThanBooleanQuery_Int)
														{
															variable=((moreThanlessThanBooleanQuery_Int) queries.get(patternDone)).process(currentDataMap);
														}
														else if(queries.get(patternDone) instanceof EqualToBooleanQuery_Float)
														{
															variable=((EqualToBooleanQuery_Float) queries.get(patternDone)).process(currentDataMap);
														}
														else if(queries.get(patternDone) instanceof notEqualToBooleanQuery_Float)
														{
															variable=((notEqualToBooleanQuery_Float) queries.get(patternDone)).process(currentDataMap);
														}
														else if(queries.get(patternDone) instanceof moreThanlessThanBooleanQuery_Float)
														{
															variable=((moreThanlessThanBooleanQuery_Float) queries.get(patternDone)).process(currentDataMap);
														}
														else if(queries.get(patternDone) instanceof EqualToBooleanQuery_Double)
														{
															variable=((EqualToBooleanQuery_Double) queries.get(patternDone)).process(currentDataMap);
														}
														else if(queries.get(patternDone) instanceof notEqualToBooleanQuery_Double)
														{
															variable=((notEqualToBooleanQuery_Double) queries.get(patternDone)).process(currentDataMap);
														}
														else if(queries.get(patternDone) instanceof moreThanlessThanBooleanQuery_Double)
														{
															variable=((moreThanlessThanBooleanQuery_Double) queries.get(patternDone)).process(currentDataMap);
														}
														else if(queries.get(patternDone) instanceof EqualToBooleanQuery_Long)
														{
															variable=((EqualToBooleanQuery_Long) queries.get(patternDone)).process(currentDataMap);
														}
														else if(queries.get(patternDone) instanceof notEqualToBooleanQuery_Long)
														{
															variable=((notEqualToBooleanQuery_Long) queries.get(patternDone)).process(currentDataMap);
														}
														else if(queries.get(patternDone) instanceof moreThanlessThanBooleanQuery_Long)
														{
															variable=((moreThanlessThanBooleanQuery_Long) queries.get(patternDone)).process(currentDataMap);
														}
														if(variable==true)
														{
															patternDone++;
														}
																		
													}
													
													//now checking for the case of first pattern..i.e if pattern starts with 'a',the next one in the 
													//sequence can be 'a' or the next one
													if(patternDone==1 && patternMatch!=1)
													{
														if(variable==false)
														{
															if(queries.get(0) instanceof EqualToBooleanQuery_String)
															{
																variable=((EqualToBooleanQuery_String) queries.get(0)).process(currentDataMap);
															}
															else if(queries.get(0) instanceof notEqualToBooleanQuery_String)
															{
																variable=((notEqualToBooleanQuery_String) queries.get(0)).process(currentDataMap);
															}
															else if(queries.get(0) instanceof notEqualToBooleanQuery_Int)
															{
																variable=((notEqualToBooleanQuery_Int) queries.get(0)).process(currentDataMap);
															}
															else if(queries.get(0) instanceof EqualToBooleanQuery_Int)
															{
																variable=((EqualToBooleanQuery_Int) queries.get(0)).process(currentDataMap);
															}
															else if(queries.get(0) instanceof moreThanlessThanBooleanQuery_Int)
															{
																variable=((moreThanlessThanBooleanQuery_Int) queries.get(0)).process(currentDataMap);
															}
															else if(queries.get(0) instanceof EqualToBooleanQuery_Float)
															{
																variable=((EqualToBooleanQuery_Float) queries.get(0)).process(currentDataMap);
															}
															else if(queries.get(0) instanceof notEqualToBooleanQuery_Float)
															{
																variable=((notEqualToBooleanQuery_Float) queries.get(0)).process(currentDataMap);
															}
															else if(queries.get(0) instanceof moreThanlessThanBooleanQuery_Float)
															{
																variable=((moreThanlessThanBooleanQuery_Float) queries.get(0)).process(currentDataMap);
															}
															else if(queries.get(0) instanceof EqualToBooleanQuery_Double)
															{
																variable=((EqualToBooleanQuery_Double) queries.get(0)).process(currentDataMap);
															}
															else if(queries.get(0) instanceof notEqualToBooleanQuery_Double)
															{
																variable=((notEqualToBooleanQuery_Double) queries.get(0)).process(currentDataMap);
															}
															else if(queries.get(0) instanceof moreThanlessThanBooleanQuery_Double)
															{
																variable=((moreThanlessThanBooleanQuery_Double) queries.get(0)).process(currentDataMap);
															}
															else if(queries.get(0) instanceof EqualToBooleanQuery_Long)
															{
																variable=((EqualToBooleanQuery_Long) queries.get(0)).process(currentDataMap);
															}
															else if(queries.get(0) instanceof notEqualToBooleanQuery_Long)
															{
																variable=((notEqualToBooleanQuery_Long) queries.get(0)).process(currentDataMap);
															}
															else if(queries.get(0) instanceof moreThanlessThanBooleanQuery_Long)
															{
																variable=((moreThanlessThanBooleanQuery_Long) queries.get(0)).process(currentDataMap);
															}
														}
														if(variable==true)
														{
															matchedEvents.clear(); 
														}
														
													}
														
														
													if(variable==true)
													{
														boolean_holder=true;
														matchedEvents.add(new TimeEventPair(currentTimestamp,currentData));
						
													}
													else
													{
														patternDone=0;
														matchedEvents.clear(); 
														boolean_holder=false;
													}
									
									                 
								}
									
									
								
									if(boolean_holder==true && patternDone==queries.size()){
										patternDone=0;
										return matchedEvents;
									}                                          //return proper stored objects
									else{
										ArrayList<TimeEventPair> NO_LIST=new ArrayList<TimeEventPair>(0);
										return NO_LIST; 									//return an empty array-list	
									}	
		
		
			}
			else
			{							
										if(timeStartOffset==0)
										{
											timeStartOffset=currentTimestamp;	
										}
										if(timeStartOffset+windowTime<currentTimestamp)
										{
											timeStartOffset=currentTimestamp;
											boolean_holder=false;
											matchedEvents.clear();
											patternMatch=0;
											patternDone=0;
											
										}
										if(timeStartOffset+windowTime>=currentTimestamp)
										{
											
														if(matchedEvents.size()>=queries.size())
														{
															matchedEvents.clear();
														}
														patternMatch++;
														boolean variable=false;
														if(BooleanQuery.class.isAssignableFrom(queries.get(patternDone).getClass()))
														{
															if(queries.get(patternDone) instanceof EqualToBooleanQuery_String)
															{
																variable=((EqualToBooleanQuery_String) queries.get(patternDone)).process(currentDataMap);
															}
															else if(queries.get(patternDone) instanceof notEqualToBooleanQuery_String)
															{
																variable=((notEqualToBooleanQuery_String) queries.get(patternDone)).process(currentDataMap);
															}
															else if(queries.get(patternDone) instanceof notEqualToBooleanQuery_Int)
															{
																variable=((notEqualToBooleanQuery_Int) queries.get(patternDone)).process(currentDataMap);
															}
															else if(queries.get(patternDone) instanceof EqualToBooleanQuery_Int)
															{
																variable=((EqualToBooleanQuery_Int) queries.get(patternDone)).process(currentDataMap);
															}
															else if(queries.get(patternDone) instanceof moreThanlessThanBooleanQuery_Int)
															{
																variable=((moreThanlessThanBooleanQuery_Int) queries.get(patternDone)).process(currentDataMap);
															}
															else if(queries.get(patternDone) instanceof EqualToBooleanQuery_Float)
															{
																variable=((EqualToBooleanQuery_Float) queries.get(patternDone)).process(currentDataMap);
															}
															else if(queries.get(patternDone) instanceof notEqualToBooleanQuery_Float)
															{
																variable=((notEqualToBooleanQuery_Float) queries.get(patternDone)).process(currentDataMap);
															}
															else if(queries.get(patternDone) instanceof moreThanlessThanBooleanQuery_Float)
															{
																variable=((moreThanlessThanBooleanQuery_Float) queries.get(patternDone)).process(currentDataMap);
															}
															else if(queries.get(patternDone) instanceof EqualToBooleanQuery_Double)
															{
																variable=((EqualToBooleanQuery_Double) queries.get(patternDone)).process(currentDataMap);
															}
															else if(queries.get(patternDone) instanceof notEqualToBooleanQuery_Double)
															{
																variable=((notEqualToBooleanQuery_Double) queries.get(patternDone)).process(currentDataMap);
															}
															else if(queries.get(patternDone) instanceof moreThanlessThanBooleanQuery_Double)
															{
																variable=((moreThanlessThanBooleanQuery_Double) queries.get(patternDone)).process(currentDataMap);
															}
															else if(queries.get(patternDone) instanceof EqualToBooleanQuery_Long)
															{
																variable=((EqualToBooleanQuery_Long) queries.get(patternDone)).process(currentDataMap);
															}
															else if(queries.get(patternDone) instanceof notEqualToBooleanQuery_Long)
															{
																variable=((notEqualToBooleanQuery_Long) queries.get(patternDone)).process(currentDataMap);
															}
															else if(queries.get(patternDone) instanceof moreThanlessThanBooleanQuery_Long)
															{
																variable=((moreThanlessThanBooleanQuery_Long) queries.get(patternDone)).process(currentDataMap);
															}
															if(variable==true)
															{
																patternDone++;
															}
																			
														}
														//now checking for the case of first pattern..i.e if pattern starts with 'a',the next one in the 
														//sequence can be 'a' or the next one
														if(patternDone==1 && patternMatch!=1)
														{
															if(variable==false)
															{
																if(queries.get(0) instanceof EqualToBooleanQuery_String)
																{
																	variable=((EqualToBooleanQuery_String) queries.get(0)).process(currentDataMap);
																}
																else if(queries.get(0) instanceof notEqualToBooleanQuery_String)
																{
																	variable=((notEqualToBooleanQuery_String) queries.get(0)).process(currentDataMap);
																}
																else if(queries.get(0) instanceof notEqualToBooleanQuery_Int)
																{
																	variable=((notEqualToBooleanQuery_Int) queries.get(0)).process(currentDataMap);
																}
																else if(queries.get(0) instanceof EqualToBooleanQuery_Int)
																{
																	variable=((EqualToBooleanQuery_Int) queries.get(0)).process(currentDataMap);
																}
																else if(queries.get(0) instanceof moreThanlessThanBooleanQuery_Int)
																{
																	variable=((moreThanlessThanBooleanQuery_Int) queries.get(0)).process(currentDataMap);
																}
																else if(queries.get(0) instanceof EqualToBooleanQuery_Float)
																{
																	variable=((EqualToBooleanQuery_Float) queries.get(0)).process(currentDataMap);
																}
																else if(queries.get(0) instanceof notEqualToBooleanQuery_Float)
																{
																	variable=((notEqualToBooleanQuery_Float) queries.get(0)).process(currentDataMap);
																}
																else if(queries.get(0) instanceof moreThanlessThanBooleanQuery_Float)
																{
																	variable=((moreThanlessThanBooleanQuery_Float) queries.get(0)).process(currentDataMap);
																}
																else if(queries.get(0) instanceof EqualToBooleanQuery_Double)
																{
																	variable=((EqualToBooleanQuery_Double) queries.get(0)).process(currentDataMap);
																}
																else if(queries.get(0) instanceof notEqualToBooleanQuery_Double)
																{
																	variable=((notEqualToBooleanQuery_Double) queries.get(0)).process(currentDataMap);
																}
																else if(queries.get(0) instanceof moreThanlessThanBooleanQuery_Double)
																{
																	variable=((moreThanlessThanBooleanQuery_Double) queries.get(0)).process(currentDataMap);
																}
																
																else if(queries.get(0) instanceof EqualToBooleanQuery_Long)
																{
																	variable=((EqualToBooleanQuery_Long) queries.get(0)).process(currentDataMap);
																}
																else if(queries.get(0) instanceof notEqualToBooleanQuery_Long)
																{
																	variable=((notEqualToBooleanQuery_Long) queries.get(0)).process(currentDataMap);
																}
																else if(queries.get(0) instanceof moreThanlessThanBooleanQuery_Long)
																{
																	variable=((moreThanlessThanBooleanQuery_Long) queries.get(0)).process(currentDataMap);
																}
															}
															if(variable==true)
															{
																matchedEvents.clear(); 
															}
															
														}
															
	
														if(variable==true)
														{
															boolean_holder=true;
															matchedEvents.add(new TimeEventPair(currentTimestamp,currentData));
							
														}
														else
														{
															patternDone=0;
															patternMatch=0;
															matchedEvents.clear(); 
															boolean_holder=false;
														}
														
										                 
									}
										
										
									
										if(boolean_holder==true && patternDone==queries.size()){
											patternDone=0;
											patternMatch=0;
											return matchedEvents;							 //return proper stored objects
										}                                          
										else
										{
											ArrayList<TimeEventPair> NO_LIST=new ArrayList<TimeEventPair>(0);
											return NO_LIST;									//return an empty array-list	
										}		
	
			}
		
		}
		
		
		
}
