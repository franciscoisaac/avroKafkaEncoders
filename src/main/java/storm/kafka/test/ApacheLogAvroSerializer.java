package storm.kafka.test;




import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;



/**
 * Avro serializer that uses the generic representation for Avro data. This
 * representation is best for applications which deal with dynamic data, whose
 * schemas are not known until runtime.
 * 
 */
public class ApacheLogAvroSerializer implements kafka.serializer.Encoder  {

	private final String HOSTREMOTO="host";
	private final String NOMBRELOGREMOTO="log";
	private final String USUARIOREMOTO="user";
	private final String TIEMPOEJECPETICION="datetime";
	private final String LINEAPETICION="request";
	private final String ESTADOPETICION="status";
	private final String TAMAÑORESPUESTA="size";
	private final String REFERER="referer";
	private final String USERAGENT="userAgent";
	private final String IDSESION="session";
	private final String TIEMPORESPUESTA="responseTime";
	
	
	private  final String[] FIELDS ={
			HOSTREMOTO,
			NOMBRELOGREMOTO,
			USUARIOREMOTO,
			TIEMPOEJECPETICION,
			LINEAPETICION,
			ESTADOPETICION,
			TAMAÑORESPUESTA,
			REFERER,
			USERAGENT,
			IDSESION,
			TIEMPORESPUESTA	
	};
	
//    private final Schema typeDef;

    /**
     * Constructor accepting the schema definition as a JSON string.
     * 
     * @param schema a serialized JSON object representing a Avro schema.
     */
    public ApacheLogAvroSerializer(kafka.utils.VerifiableProperties vps) {
       System.out.println("vps: " + vps.toString());
    }
    

    private Schema schema;
    
    public ApacheLogAvroSerializer() {
    	setSchema();
	}
    
    private void setSchema() {
        Schema.Parser parser = new Schema.Parser();
        try {
			schema = parser.parse(getClass().getClassLoader().getResourceAsStream("apacheLog.avsc"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public ApacheLogAvroSerializer(Schema sch) {
			schema = sch;
	}

	public byte[] toBytes(String str) {

		if(schema==null) setSchema();
		
      
        GenericRecord datum = new GenericData.Record(schema);
        StringTokenizer st = new StringTokenizer(str, " ");
        int i=0;
        while(st.hasMoreTokens()) {        
        	datum.put(FIELDS[i++],st.nextElement());
        }


        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try {
			writer.write(datum, encoder);
	        encoder.flush();	        
	        System.out.println("encoded string: " + out.toString());
	        out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        
        System.out.println(out.size());
        return out.toByteArray();	
        
    }

	public byte[] toBytes(Object arg0) {
		return this.toBytes(arg0.toString());
	}

}
