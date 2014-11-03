package cep.query.booleanQuery;

import java.util.ArrayList;
import java.util.HashMap;

public class moreThanlessThanBooleanQuery_Long extends BooleanQuery{
	int parameter;
	public moreThanlessThanBooleanQuery_Long(String in_query,int queryParameters)
	{
		super(in_query);
		this.parameter=queryParameters; 
	}
	public boolean process(HashMap<String,String> currentDataMap)
	{
		query=query.trim();
		//System.out.println(currentDataMap.toString());  
		String[] parametersNames=null;
		if(query.contains(">"))
			parametersNames=query.split(">");
		else
			parametersNames=query.split("<");
		parametersNames[0]=parametersNames[0].replaceAll("\\s+","");
		parametersNames[1]=parametersNames[1].replaceAll("\\s+","");
		ArrayList<Long> parametersStrings=new ArrayList<Long>();
		if(parameter==0)
		{		
			parametersStrings.add(Long.parseLong(currentDataMap.get(parametersNames[0])));
			parametersStrings.add(Long.parseLong(currentDataMap.get(parametersNames[1])));
		}
		else
		{	
			String a=parametersNames[0];
			parametersStrings.add(Long.parseLong(currentDataMap.get(a)));
			String userGivenValue=parametersNames[1].replace("(","");
			userGivenValue=userGivenValue.replace(")","");
			parametersStrings.add(Long.parseLong(userGivenValue));
		}
		
		//checking for equality
		if(query.contains(">"))
			return (parametersStrings.get(0).longValue() > parametersStrings.get(1).longValue());
		else
			return (parametersStrings.get(0).longValue() < parametersStrings.get(1).longValue());
	}

}

