package cep.query.booleanQuery;

import java.util.HashMap;

import cep.query.Query;

abstract public class BooleanQuery implements Query{
	protected String query; 
	public BooleanQuery(String in_query)
	{
		this.query=in_query;
		//System.out.println(this.query); 
	}
	abstract public boolean process(HashMap<String,String> currentDataMap);
}
