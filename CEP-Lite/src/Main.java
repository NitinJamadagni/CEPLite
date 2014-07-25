import java.net.UnknownHostException;
import cep.core.CEPManager;
import cep.core.InputHandler;
import cep.core.OutputHandler;
import cep.core.SourceStream;
import cep.core.TimeEventPair;
import cep.core.differentTypesException;
import cep.core.lessArgumentsException;
import cep.inputParser.stringInputParser.StringInputParser;


public class Main {
	
	
	public static void main(String[] args) throws differentTypesException, UnknownHostException, lessArgumentsException, InterruptedException {
		// TODO Auto-generated method stub
		
		CEPManager manager=new CEPManager();
		//setting the server address for the output to be sent..setting can be either 'localhost' or 'x.x.x.x' or null
		manager.setServerAddress("10.156.14.140",OutputHandler.HTTP);   
		manager.addStreamSource("Stream1","String name,int age,int weight,String surname,int grade");
		manager.addStreamSource("Stream2", "String surname,int age,int weight,int grade");
		manager.createEventQueue("csvdata");
		SourceStream stream2=manager.getSourceStream("Stream2");
		SourceStream stream1=manager.getSourceStream("Stream1");
		InputHandler handler=stream1.getInputHandler();
		InputHandler handler2=stream2.getInputHandler();
		handler.setOutputHandler(stream2,"surname,age,weight,grade");
		//handler2.setOutputHandler(null,"sum(weight)+3,avg(age)-1,sum(age),max(grade)+2#window.length(5)");  
		//handler2.addQuery("pat{age==(10),age==(20),age==(30)}#window.length(5)");
		handler2.addQuery("age==(10) and weight!=(20)"); 
		handler.addQuery("[[name==(nitin) and age!=(0)] and surname==(Jamadagni)]");
		stream1.setQueueId(null);                                  //starts the stream........
		stream2.setQueueId(null);                                  //starts the stream........
		StringInputParser parser=new StringInputParser();
		for(int i=0;i<10;i++)
		{
			manager.addEventToQueue("Stream1",new TimeEventPair(System.currentTimeMillis(),parser.parse("nitin,10,50,Jamadagni,1")));
		}
		
	}

}
