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
 
public class ApacheLogAvroProducer {
	static final Logger logger = Logger.getLogger(App.class);
	
    public static void main(String[] args) {

    	logger.info("start");
 
        Properties props = new Properties();
        props.put("metadata.broker.list", "pivhdsne:9092");

        ApacheLogAvroSerializer ass=new ApacheLogAvroSerializer();
        Producer<String, byte[]> producer = new kafka.javaapi.producer.Producer<String, byte[]>(new ProducerConfig(props));
        	
       
		int aux=800;
		for(int i=aux;i<aux+20;i++) {
	    	Message message=null;
	
					String HOSTREMOTO="85.155.188.197";
					String NOMBRELOGREMOTO="-";
					String USUARIOREMOTO="user" + i;
					String TIEMPOEJECPETICION="[17/Sep/2012:19:01:24+0200]";
					String LINEAPETICION="\"GET_/Estatico/Globales/V114/Bhtcs/Internet/AT/\"";
					String ESTADOPETICION="200";
					String TAMAÑORESPUESTA="3117";
					String REFERER="\"-\"";
					String USERAGENT="\"Chrome/21.0.1180.89\"";
					String IDSESION="\"0000z2ur1hruUUG-MhpsITK9JY_:16vnisqka\"";
					String TIEMPORESPUESTA="1020";
					
					String payload=
							 HOSTREMOTO + " " +
							 NOMBRELOGREMOTO + " " +
							 USUARIOREMOTO + " " +
							 TIEMPOEJECPETICION + " " +
							 LINEAPETICION + " " +
							 ESTADOPETICION + " " +
							 TAMAÑORESPUESTA + " " +
							 REFERER + " " +
							 USERAGENT + " " +
							 IDSESION + " " +
							 TIEMPORESPUESTA;		
					
				
				message = new Message(ass.toBytes(payload));//,key);

				System.out.println("buffer size: " + message.buffer().array().length);
				System.out.println("payload size: " + message.payloadSize());
				

				
				producer.send(new KeyedMessage<String, byte[]>("_2_2_avro", message.buffer().array()));	
				System.out.println("buffer txt bytes: " + new String(message.buffer().array()));
				System.out.println("buffer txt bytes: " + new String(message.payload().array()));

       
    	}
        producer.close();
        
        logger.info("end");
    }
}