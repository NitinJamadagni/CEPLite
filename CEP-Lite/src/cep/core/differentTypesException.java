package cep.core;

public class differentTypesException extends Exception{
	private static final long serialVersionUID = 1L;
	private String message=null;
	public differentTypesException(String message) {
		super(message);
	}
	public differentTypesException(Throwable cause)
	{
		super(cause);
	}
	@Override
	public String toString()
	{
		return message;
	}
	@Override
	public String getMessage()
	{
		return message;
	}
	
}
