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
		
		//Create a manager for your StreamProcessors
		CEPManager manager=new CEPManager();
		
		//setting the server address for the output to be sent..setting can be either 'localhost' or 'x.x.x.x' or null
		manager.setServerAddress("10.156.14.140",OutputHandler.HTTP);   //Can be changed to OutputHandler.COAP for using the coap protocol for communication with server   
		
		//add some StreamProcessing units,give their names and then the schema for input(along with datatypes)
		manager.addStreamSource("Stream1","String name,int age,int weight,String surname,int grade");
		manager.addStreamSource("Stream2", "String surname,int age,int weight,int grade");
		
		//create a event queue from where the StreamProcessors poll.Optional,if not created,then a queue
		//with the name of the StreamProcessor will be created from where events will be polled.
		//Each StreamProcessor will have an associated queue
		manager.createEventQueue("csvdata");
		
		SourceStream stream2=manager.getSourceStream("Stream2");
		SourceStream stream1=manager.getSourceStream("Stream1");
		InputHandler handler=stream1.getInputHandler();
		InputHandler handler2=stream2.getInputHandler();
		//set the outputHandler for the StreamProcessor with the arguments
		//	1.StreamProcessor name it has to output to,if given null then will be sent to the server with the defined protocol
		//	2.Schema of output.	
		handler.setOutputHandler(stream2,"surname,age,weight,grade");
		
		//to perform sum,max,avg over a window of number/time of events follow as below(window time in milliseconds)
		//handler2.setOutputHandler(null,"sum(weight)+3,avg(age)-1,sum(age),max(grade)+2#window.length(5)");
		
		//Adding queries to the StreamProcessor's InputHandler	
		//Filter queries.
		handler2.addQuery("age==(10) and weight!=(20)"); 
		handler.addQuery("[[name==(nitin) and age!=(0)] and surname==(Jamadagni)]");
		//Pattern or sequence detection(sequence is continous sequence in a given window)
		//handler2.addQuery("pat{age==(10),age==(20),age==(30)}#window.length(5)");
		//handler.addQuery("seq{age==(10),age==(20),age==(30)}#window.length(5)");
		
		//set the name of the queue from which the Processor will poll,if none give null
		stream1.setQueueId(null);                                  //starts the stream........
		stream2.setQueueId(null);                                  //starts the stream........
		StringInputParser parser=new StringInputParser();          //parser to convert the comma separated string to array of Strings 
 		for(int i=0;i<10;i++)
		{
			manager.addEventToQueue("Stream1",new TimeEventPair(System.currentTimeMillis(),parser.parse("nitin,10,50,Jamadagni,1")));
		}
		
	}

}
