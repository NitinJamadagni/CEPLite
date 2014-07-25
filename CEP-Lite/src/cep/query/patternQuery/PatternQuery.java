package cep.query.patternQuery;

import java.util.ArrayList;
import java.util.HashMap;

import cep.core.TimeEventPair;
import cep.query.Query;

abstract public class PatternQuery implements Query{
	protected String query; 
	public PatternQuery(String in_query)
	{
		this.query=in_query;
	}
	abstract public ArrayList<TimeEventPair> process(HashMap<String,String> currentDataMap); 
}
