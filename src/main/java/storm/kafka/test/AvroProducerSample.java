package storm.kafka.test;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;
 
public class AvroProducerSample {
	static final Logger logger = Logger.getLogger(App.class);
	
    public static void main(String[] args) {

    	logger.info("start");
 
        Properties props = new Properties();
        props.put("metadata.broker.list", "pivhdsne:9092");

        AvroNoSchemaSerializer ass=new AvroNoSchemaSerializer();
        Producer<String, byte[]> producer = new kafka.javaapi.producer.Producer<String, byte[]>(new ProducerConfig(props));
        
	
		
		try {
			FileOutputStream os = new FileOutputStream(new File("test.avro"));
			try {
				for(int k=0;k<10;k++)
				os.write(ass.toBytes( k + ": mas el texto que quiero meter en avro"));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				try {
					os.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    
        
       for(int i=0;i<20;i++) {
	    	   Message message=null;
			try {
				//byte[] key= { 1, 1, 1, 1};
				message = new Message(ass.toBytes(i + ": mas el texto que quiero meter en avro"));//,key);

				System.out.println("buffer size: " + message.buffer().array().length);
				System.out.println("payload size: " + message.payloadSize());
				

//
//				byte[] ba = outputStream.toByteArray( );
				
//				byte[] ba = new byte[message.buffer().array().length + message.payloadSize()];
//				
//				System.arraycopy(message.buffer().array(), 0, ba, 0, message.buffer().array().length);
//				System.arraycopy(message.payload().array(), 0, ba, message.buffer().array().length, message.payloadSize());

	
//				ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//				outputStream.write( message.buffer().array() );
//				outputStream.write( message.payload().array() );
//
//				byte[] ba = outputStream.toByteArray( );
//				
//				System.out.println("msg bytes: " + new String(ba));
//				for(int j=0;j<ba.length;j++) System.out.println("byte " + j + ": " + ba[j]);
//				System.out.println("msg size " + ba.length);
//				producer.send(new KeyedMessage<String, byte[]>("12avro", ba));	
				
				producer.send(new KeyedMessage<String, byte[]>("_1_1_avro", message.buffer().array()));	
				System.out.println("buffer txt bytes: " + new String(message.buffer().array()));
				System.out.println("buffer txt bytes: " + new String(message.payload().array()));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
       
    	}
        producer.close();
        
        logger.info("end");
    }
}