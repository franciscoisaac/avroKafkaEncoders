package storm.kafka.test;




import java.io.ByteArrayOutputStream;
import java.io.IOException;

import kafka.serializer.Decoder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;



/**
 * Avro serializer that uses the generic representation for Avro data. This
 * representation is best for applications which deal with dynamic data, whose
 * schemas are not known until runtime.
 * 
 */
public class AvroNoSchemaSerializer implements kafka.serializer.Encoder  {

//    private final Schema typeDef;

    /**
     * Constructor accepting the schema definition as a JSON string.
     * 
     * @param schema a serialized JSON object representing a Avro schema.
     */
    public AvroNoSchemaSerializer(kafka.utils.VerifiableProperties vps) {
       System.out.println("vps: " + vps.toString());
    }
    
//    public AvroStringSerializer(String schema) {
////        typeDef = Schema.parse(schema);
//    }

    public AvroNoSchemaSerializer() {
		// TODO Auto-generated constructor stub
	}

	public byte[] toBytes(String str) throws IOException {


        Schema schema = Schema.parse("{\"type\": \"record\", " +
                "\"name\": \"StringHolder\", " +
                "\"fields\": " +
                "[{\"name\":\"value\", \"type\": \"string\"}]}");


        GenericRecord datum = new GenericData.Record(schema);
        datum.put("value", str);


        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(datum, encoder);
        encoder.flush();
        
        System.out.println("encoded string: " + out.toString());
        out.close();
      
        byte[] ba= out.toByteArray();
        System.out.println("encoded string: " + new String(ba));

        
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(ba, null);
        GenericRecord result = reader.read(null, decoder);

        
        System.out.println("decoded string: " + result.get("value").toString());

            return ba;	
        
    }


	public byte[] toBytes(Object arg0) {
		 byte[] result=null;
		try {
			result= this.toBytes(arg0.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}

}
