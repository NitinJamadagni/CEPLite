package cep.inputParser.stringInputParser;
public class StringInputParser {
	public String[] parse(String input_Query)
	{
		String[] query=input_Query.split(",");
		return query;
	}

}
