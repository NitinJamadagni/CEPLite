package cep.query.booleanQuery;

import java.util.HashMap;
import java.util.LinkedHashMap;

import cep.core.differentTypesException;
import cep.query.Query;

public class ComplexPredicateQuery extends BooleanQuery {
		//String query;
		HashMap<String,String> currentDataMap;
		LinkedHashMap<String,String> schemaVariables;
		public ComplexPredicateQuery(String in_query)
		{
			super(in_query);
			query=in_query.replace(" and ","&&").replace(" or ","||").replace(" not ","!");
			query=query.replace("not ","!").replace(" not","!").replace("not","!"); 
			currentDataMap=null;
			schemaVariables=null;
		}
		@Override
		public boolean process(HashMap<String,String> useless){return false;}
		public boolean process(HashMap<String,String> in_currentDataMap,LinkedHashMap<String,String> in_schemaVariables) throws differentTypesException
		{
			currentDataMap=in_currentDataMap;
			//System.out.println(currentDataMap.toString()); 
			schemaVariables=in_schemaVariables;
			//System.out.println(schemaVariables.toString()); 
			boolean variable=linear_process();
			return variable;
		}
		private boolean linear_process() throws differentTypesException
		{
				//String currentBoolean="";
				Boolean current_Boolean=null;
				String operator="";
				boolean not_indiactaor=false;
				int i=0;
				/* not operator support not added yet
				 * assumption that there are no spaces in the query
				 * [] given bracket precedence
				 * logic:if operator=[ then push into stack
				 		 else if operator=] the pop till the first ] and make it the current boolean value 	
						 else if operator=and/or then make it the current operator(taking advantage of the fact that and/or have same precedence and are associative)
						 else evaluate the boolean query and add it to the current boolean value
				*/
				int len=query.length();
				while(i<len)
				{
					
					if(query.charAt(i)=='[')
					{
						//push into stack
						i++;
						continue;
					}
					else if(query.charAt(i)=='!')
					{
						not_indiactaor=!not_indiactaor;
						i++;
					}
					else if(query.charAt(i)=='&')
					{
						//System.out.println("Entered and check");
						if(not_indiactaor)
						{
							not_indiactaor=false;
							/*if(currentBoolean=="true"){currentBoolean="false";}
							else if(currentBoolean=="false"){currentBoolean="true";}*/
							current_Boolean=!current_Boolean;
						}
//---------------------------------optimization-trial----------------------------------
								
								//if(currentBoolean=="false")
								if(current_Boolean==false)	
								{
									int j=i+2;
									boolean or_indicator=false;
									int len1=query.length();
									char a=query.charAt(j);
									while(j<len1 && a!=']')
									{
										if(query.charAt(j)=='[')
										{
											while(query.charAt(j)!=']')
											{
												j++;
											}
											
										}
										if(query.charAt(j)=='|')
										{
											or_indicator=true;
											break;
										}
										j++;
										a=query.charAt(j);
									}
									if(!or_indicator)
									{
										i=j+1;
										break;
									}
								}
//---------------------------------optimization-trial----------------------------------
						operator="and";
						i=i+2;
					}
					else if(query.charAt(i)=='|')
					{
						//System.out.println("Enterde or check"); 
						if(not_indiactaor)
						{
							not_indiactaor=false;
							/*if(currentBoolean=="true"){currentBoolean="false";}
							else if(currentBoolean=="false"){currentBoolean="true";}*/
							current_Boolean=!current_Boolean;
						}
//---------------------------------optimization-trial----------------------------------
									//if(currentBoolean=="true")
									if(current_Boolean==true)
									{
										int j=i+2;
										boolean and_indicator=false;
										int len1=query.length();
										char a=query.charAt(j);
										while(j<len1 && a!=']')
										{
											if(query.charAt(j)=='[')
											{
												while(query.charAt(j)!=']')
												{
													j++;
												}
												
											}
											if(query.charAt(j)=='&')
											{
												and_indicator=true;
												break;
											}
											j++;
											a=query.charAt(j);
										}
										if(!and_indicator)
										{
											i=j+1;
											break;
										}
									}
//-----------------------------------optimization-trial--------------------------------
						
						operator="or";
						i=i+2;
					}
					else if(query.charAt(i)==']')
					{
						//pop from stack all the boolean value till we get [
						if(not_indiactaor)
						{
							not_indiactaor=false;
							/*if(currentBoolean=="true"){currentBoolean="false";}
							else if(currentBoolean=="false"){currentBoolean="true";}*/
							current_Boolean=!current_Boolean;
						}
						i++;
						continue;
					}
					else		//when it has to be a boolean expression
					{
							String currentQuery="";
							StringBuffer temp=new StringBuffer();
							/*
							while(i<query.length() && (query.charAt(i)!='&'||query.charAt(i)!='|'||query.charAt(i)!=']'||query.charAt(i)!='[')  )
							{
								currentQuery+=query.charAt(i);
								i++;
							}*/
							while(true)
							{
								if(i>query.length()-1)
									break;
								if((query.charAt(i)=='&'||query.charAt(i)=='|'||query.charAt(i)==']'||query.charAt(i)=='['||(query.charAt(i)=='!' && query.charAt(i+1)!='=')) )
									break;
								//currentQuery+=query.charAt(i);
								temp.append(query.charAt(i));
								i++;
							}
							currentQuery=temp.toString();
							//System.out.println("Current query is:"+currentQuery);
							Query queries=null;
							//now get the output from the boolean query
//----------------------------------------------------------------------------------------------------------							
							//add other boolean-query parsings later
							if(currentQuery.contains("=="))
							{			 
										//checking the parameters type
										currentQuery=currentQuery.trim();
										currentQuery=currentQuery.replaceAll("\\s+"," ");
										String[] parametersVars=currentQuery.split("==");
										int queryParameter=0;
										//explicit mentions of compare criteria
										if(  currentQuery.contains( "(")  &&  currentQuery.contains(")") )
										{
											
													queryParameter=1;
													//add all other data types
													if(schemaVariables.get(parametersVars[0]).contains("String"))
													{	
														queries=(new EqualToBooleanQuery_String(currentQuery,queryParameter));
													}
													else if(schemaVariables.get(parametersVars[0]).contains("int"))
													{
														queries=(new EqualToBooleanQuery_Int(currentQuery,queryParameter));
													}
													else if(schemaVariables.get(parametersVars[0]).contains("double"))
													{
														queries=(new EqualToBooleanQuery_Double(currentQuery,queryParameter));
													}
													else if(schemaVariables.get(parametersVars[0]).contains("float"))
													{
														queries=(new EqualToBooleanQuery_Float(currentQuery,queryParameter));
													}
													else if(schemaVariables.get(parametersVars[0]).contains("long"))
													{
														queries=(new EqualToBooleanQuery_Long(currentQuery,queryParameter));
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
															queries=(new EqualToBooleanQuery_String(currentQuery,queryParameter));
														}
														else if(schemaVariables.get(parametersVars[0]).contains("int"))
														{
															queries=(new EqualToBooleanQuery_Int(currentQuery,queryParameter));
														}
														else if(schemaVariables.get(parametersVars[0]).contains("double"))
														{
															queries=(new EqualToBooleanQuery_Double(currentQuery,queryParameter));
														}
														else if(schemaVariables.get(parametersVars[0]).contains("float"))
														{
															queries=(new EqualToBooleanQuery_Float(currentQuery,queryParameter));
														}
														else if(schemaVariables.get(parametersVars[0]).contains("long"))
														{
															queries=(new EqualToBooleanQuery_Long(currentQuery,queryParameter));
														}
													}
													else
													{
														throw new differentTypesException("Wrong query(different types not comparable!)");
													}
										}
							}
							else if(currentQuery.contains("!="))
							{			 
										//checking the parameters type
										currentQuery=currentQuery.trim();
										currentQuery=currentQuery.replaceAll("\\s+"," ");
										String[] parametersVars=currentQuery.split("!=");
										int queryParameter=0;
										//explicit mentions of compare criteria
										if(  currentQuery.contains( "(")  &&  currentQuery.contains(")") )
										{
											
													queryParameter=1;
													//add all other data types
													if(schemaVariables.get(parametersVars[0]).contains("String"))
													{	
														queries=(new notEqualToBooleanQuery_String(currentQuery,queryParameter));
													}
													else if(schemaVariables.get(parametersVars[0]).contains("int"))
													{
														queries=(new notEqualToBooleanQuery_Int(currentQuery,queryParameter));
													}
													else if(schemaVariables.get(parametersVars[0]).contains("double"))
													{
														queries=(new notEqualToBooleanQuery_Double(currentQuery,queryParameter));
													}
													else if(schemaVariables.get(parametersVars[0]).contains("float"))
													{
														queries=(new notEqualToBooleanQuery_Float(currentQuery,queryParameter));
													}
													else if(schemaVariables.get(parametersVars[0]).contains("long"))
													{
														queries=(new notEqualToBooleanQuery_Long(currentQuery,queryParameter));
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
															queries=(new notEqualToBooleanQuery_String(currentQuery,queryParameter));
														}
														else if(schemaVariables.get(parametersVars[0]).contains("int"))
														{
															queries=(new notEqualToBooleanQuery_Int(currentQuery,queryParameter));
														}
														else if(schemaVariables.get(parametersVars[0]).contains("double"))
														{
															queries=(new notEqualToBooleanQuery_Double(currentQuery,queryParameter));
														}
														else if(schemaVariables.get(parametersVars[0]).contains("float"))
														{
															queries=(new notEqualToBooleanQuery_Float(currentQuery,queryParameter));
														}
														else if(schemaVariables.get(parametersVars[0]).contains("long"))
														{
															queries=(new notEqualToBooleanQuery_Long(currentQuery,queryParameter));
														}
													}
													else
													{
														throw new differentTypesException("Wrong query(different types not comparable!)");
													}
										}
							}
							else if(currentQuery.contains(">") || currentQuery.contains("<")) 
							{			 
										//checking the parameters type
										currentQuery=currentQuery.trim();
										currentQuery=currentQuery.replaceAll("\\s+"," ");
										String[] parametersVars=null;
										if(currentQuery.contains(">"))	
											parametersVars=currentQuery.split(">");
										else
											parametersVars=currentQuery.split("<");
										int queryParameter=0;
										//explicit mentions of compare criteria
										if(  currentQuery.contains( "(")  &&  currentQuery.contains(")") )
										{
											
													queryParameter=1;
													//add all other data types
													if(schemaVariables.get(parametersVars[0]).contains("int"))
													{
														queries=(new moreThanlessThanBooleanQuery_Int(currentQuery,queryParameter));
													}
													else if(schemaVariables.get(parametersVars[0]).contains("double"))
													{
														queries=(new moreThanlessThanBooleanQuery_Double(currentQuery,queryParameter));
													}
													else if(schemaVariables.get(parametersVars[0]).contains("float"))
													{
														queries=(new moreThanlessThanBooleanQuery_Float(currentQuery,queryParameter));
													}
													else if(schemaVariables.get(parametersVars[0]).contains("long"))
													{
														queries=(new moreThanlessThanBooleanQuery_Long(currentQuery,queryParameter));
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
															queries=(new moreThanlessThanBooleanQuery_Int(currentQuery,queryParameter));
														}
														else if(schemaVariables.get(parametersVars[0]).contains("double"))
														{
															queries=(new moreThanlessThanBooleanQuery_Double(currentQuery,queryParameter));
														}
														else if(schemaVariables.get(parametersVars[0]).contains("float"))
														{
															queries=(new moreThanlessThanBooleanQuery_Float(currentQuery,queryParameter));
														}
														else if(schemaVariables.get(parametersVars[0]).contains("long"))
														{
															queries=(new moreThanlessThanBooleanQuery_Long(currentQuery,queryParameter));
														}
													}
													else
													{
														throw new differentTypesException("Wrong query(different types not comparable!)");
													}
										}
							}
//-----------------------------------------------------------------------------------------------------------							
							boolean variable=false;
							if(BooleanQuery.class.isAssignableFrom(queries.getClass()))
							{
								if(queries instanceof EqualToBooleanQuery_String)
								{
									variable=((EqualToBooleanQuery_String) queries).process(currentDataMap);
								}
								else if(queries instanceof notEqualToBooleanQuery_String)
								{
									variable=((notEqualToBooleanQuery_String) queries).process(currentDataMap);
								}
								else if(queries instanceof EqualToBooleanQuery_Int)
								{
									//System.out.println(currentDataMap.toString()); 
									variable=((EqualToBooleanQuery_Int) queries).process(currentDataMap);
								}
								else if(queries instanceof notEqualToBooleanQuery_Int)
								{
									variable=((notEqualToBooleanQuery_Int) queries).process(currentDataMap);
								}
								else if(queries instanceof moreThanlessThanBooleanQuery_Int)
								{
									variable=((moreThanlessThanBooleanQuery_Int) queries).process(currentDataMap);
								}
								else if(queries instanceof EqualToBooleanQuery_Float)
								{
									variable=((EqualToBooleanQuery_Float) queries).process(currentDataMap);
								}
								else if(queries instanceof notEqualToBooleanQuery_Float)
								{
									variable=((notEqualToBooleanQuery_Float) queries).process(currentDataMap);
								}
								else if(queries instanceof moreThanlessThanBooleanQuery_Float)
								{
									variable=((moreThanlessThanBooleanQuery_Float) queries).process(currentDataMap);
								}
								else if(queries instanceof EqualToBooleanQuery_Double)
								{
									variable=((EqualToBooleanQuery_Double) queries).process(currentDataMap);
								}
								else if(queries instanceof notEqualToBooleanQuery_Double)
								{
									variable=((notEqualToBooleanQuery_Double) queries).process(currentDataMap);
								}
								else if(queries instanceof moreThanlessThanBooleanQuery_Double)
								{
									variable=((moreThanlessThanBooleanQuery_Double) queries).process(currentDataMap);
								}
								else if(queries instanceof EqualToBooleanQuery_Long)
								{
									variable=((EqualToBooleanQuery_Long) queries).process(currentDataMap);
								}
								else if(queries instanceof notEqualToBooleanQuery_Long)
								{
									variable=((notEqualToBooleanQuery_Long) queries).process(currentDataMap);
								}
								else if(queries instanceof moreThanlessThanBooleanQuery_Long)
								{
									variable=((moreThanlessThanBooleanQuery_Long) queries).process(currentDataMap);
								}
							}
//------------------------------------------------------------------------------------------------------------							
							//if(currentBoolean=="")
							if(current_Boolean==null)
							{	
								/*if(variable==true)
								{
									currentBoolean="true";
								}
								else
								{
									currentBoolean="false";
								}*/
								current_Boolean=variable;
							}
							else
							{
								if(operator=="and")
								{
									/*if(currentBoolean=="true" && variable==true){currentBoolean="true";}
									else{currentBoolean="false";}*/
									if(variable==false){current_Boolean=false;}
									operator="";
								}
								else if(operator=="or")
								{
									/*if(currentBoolean=="false" && variable==false){currentBoolean="false";}
									else{currentBoolean="true";}*/
									if(variable==true){current_Boolean=true;}
									operator="";
								}
							}
					}
					
				}
				
				
				//returning the evaluated predicate
				/*if (currentBoolean.contains("true"))
				{
					return true;
				}
				else
				{
					return false;
				}*/
				return current_Boolean.booleanValue();
		}
		
}
