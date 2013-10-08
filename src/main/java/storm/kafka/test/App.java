package storm.kafka.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * Hello world!
 *
 */
public class App 
{
	static final Logger logger = Logger.getLogger(App.class);

	public static void main(String[] args)
	{
		//Configure logger
//		BasicConfigurator.configure();
//		PropertyConfigurator.configure("log4j.properties");
		//logger.debug("{    'name': 'n3',    'type': '3'}");
		
//		AvroStringSerializer ass= new AvroStringSerializer();
		
		
//		try {
//			FileOutputStream os = new FileOutputStream(new File("test.avro"));
//			try {
//				os.write(ass.toBytes("el texto que quiero meter en avro"));
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} finally {
//				try {
//					os.close();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
//			
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		
		int aux=800;
		for(int i=aux;i<aux+20;i++)
			logger.debug("texto " + i);

	}
}
