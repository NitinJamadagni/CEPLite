import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Scanner;

import cep.core.CEPManager;
import cep.core.InputHandler;
import cep.core.OutputHandler;
import cep.core.StreamProcessor;
import cep.core.TimeEventPair;
import cep.core.differentTypesException;
import cep.core.lessArgumentsException;
import cep.inputParser.stringInputParser.StringInputParser;


public class Example {
	
	
	public static void main(String[] args) throws differentTypesException, lessArgumentsException, InterruptedException, IOException {
		// TODO Auto-generated method stub
		 BufferedReader reader = new BufferedReader(new FileReader("C:\\Users\\Nitin\\Desktop\\values.txt"));
		 String line = null;
		 long  timeDiff;
		 long  prevTime = 0;
		 Long time;
		 Double height;
		 long timescale = 1;
		/*
		CEPManager manager=new CEPManager();
		//setting the server address for the output to be sent..setting can be either 'localhost' or 'x.x.x.x' or null
		manager.setServerAddress("192.168.1.100",OutputHandler.HTTP);   
		manager.addStreamProcessor("Stream1","String name,int age,int weight,String surname,int grade");
		manager.addStreamProcessor("Stream2", "String surname,int age,int weight,int grade");
		manager.createEventQueue("csvdata");
		StreamProcessor stream2=manager.getSourceStream("Stream2");
		StreamProcessor stream1=manager.getSourceStream("Stream1");
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
		}*/
		
		CEPManager manager=new CEPManager();
		manager.setServerAddress("192.168.1.100",OutputHandler.HTTP);
		manager.addStreamProcessor("HeightEventStream","long timestamp,double height");
		manager.addStreamProcessor("HeightVal","long timestamp,double height");
		manager.addStreamProcessor("NonOutlierStream","long timestamp,double height");
		manager.addStreamProcessor("CriticalMaxStream","long timestamp,double height");
		manager.createEventQueue("Events"); 
		
		StreamProcessor HeightEventStream=manager.getSourceStream("HeightEventStream");
		StreamProcessor HeightVal=manager.getSourceStream("HeightVal");
		StreamProcessor NonOutlierStream=manager.getSourceStream("NonOutlierStream");
		StreamProcessor CriticalMaxStream=manager.getSourceStream("CriticalMaxStream");
		
		
		InputHandler HeightEventStreamHandler=HeightEventStream.getInputHandler();
		HeightEventStreamHandler.setOutputHandler(HeightVal,"timestamp,height");
		InputHandler HeightValHandler=HeightVal.getInputHandler();
		HeightValHandler.setOutputHandler(NonOutlierStream,"timestamp,height");
		HeightValHandler.addQuery("height>(-108) and height<(108)");
		InputHandler NonOutlierStreamHandler=NonOutlierStream.getInputHandler();
		NonOutlierStreamHandler.setOutputHandler(CriticalMaxStream,"timestamp,height");
		NonOutlierStreamHandler.addQuery("height>(109.8)");
		NonOutlierStreamHandler.addQuery("height<(12.2)");
		
		HeightEventStream.setQueueId("Events");
		HeightVal.setQueueId(null);
		NonOutlierStream.setQueueId(null);
		CriticalMaxStream.setQueueId(null);
		StringInputParser parser=new StringInputParser();
		while ((line = reader.readLine()) != null) 
		{
			
			Scanner scanner = new Scanner(line);
			
			time = scanner.nextLong();
			
			height = scanner.nextDouble();
			
			prevTime = (prevTime == 0? time : prevTime);
			
			timeDiff = time - prevTime;
			Thread.sleep(timeDiff/timescale);
			try{
				
				manager.addEventToQueue("Events",new TimeEventPair(System.currentTimeMillis(),parser.parse(time.toString()+","+height.toString())));
			}catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		/*manager.addStreamProcessor("Stream1","String name,int age,int weight,String surname,int grade");
		manager.addStreamProcessor("Stream2", "String surname,int age,int weight,int grade");
		StreamProcessor stream1=manager.getSourceStream("Stream1");
		StreamProcessor stream2=manager.getSourceStream("Stream2");
		InputHandler handler2=stream2.getInputHandler();
		InputHandler handler=stream1.getInputHandler();
		handler.setOutputHandler(stream2,"surname,age,weight,grade");
		handler2.addQuery("age==(10) and weight!=(20)");
		stream1.setQueueId(null);                                  //starts the stream........
		stream2.setQueueId(null);
		StringInputParser parser=new StringInputParser();
		manager.addEventToQueue("Stream1",new TimeEventPair(System.currentTimeMillis(),parser.parse("nitin,10,50,Jamadagni,1")));*/
	}

}
