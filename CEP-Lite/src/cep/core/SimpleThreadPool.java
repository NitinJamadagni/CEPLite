package cep.core;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
public class SimpleThreadPool {
	//use of fixedthreadpool and cachedthreadpool can be varied based on where the application is running
	//fixedthreadpool limits the number of runnable threads to a particular number and runs them concurrently
	//if limit is reached it queues up the incoming threads until a free thread is available
	//cachedthreadpool creates new threads as needed but will reuse free threads if available
	//we have to manually limit the number of threads limit
	//basically we can use cachedthreadpool effectively if the tasks of the threads are more shortlived
	//than the rateof incoming threads.....othrewise it will be as effective as a fixedthreadpool
	
	int number_of_threads=0;
	//ExecutorService executor=Executors.newFixedThreadPool(5);       //change value later
	ExecutorService executor=Executors.newCachedThreadPool();
	public void process(Thread thread)
	{
		//System.out.println("Simplethread pool caleed on "+thread.getName()); 
		executor.execute(thread); 
	}
	
}
