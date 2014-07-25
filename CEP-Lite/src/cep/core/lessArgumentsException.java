package cep.core;

public class lessArgumentsException extends Exception{
	private static final long serialVersionUID = 1L;
	private String message=null;
	public lessArgumentsException(String message) {
		super(message);
	}
	public lessArgumentsException(Throwable cause)
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
