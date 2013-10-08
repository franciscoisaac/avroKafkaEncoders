package storm.kafka.test;




import java.io.ByteArrayOutputStream;
import java.io.IOException;

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
public class AvroStringSerializer implements kafka.serializer.Encoder  {

//    private final Schema typeDef;

    /**
     * Constructor accepting the schema definition as a JSON string.
     * 
     * @param schema a serialized JSON object representing a Avro schema.
     */
    public AvroStringSerializer(kafka.utils.VerifiableProperties vps) {
       System.out.println("vps: " + vps.toString());
    }
    
//    public AvroStringSerializer(String schema) {
////        typeDef = Schema.parse(schema);
//    }

    public AvroStringSerializer() {
		// TODO Auto-generated constructor stub
	}

	public byte[] toBytes(String str) {


        Schema schema = Schema.parse("{\"type\": \"record\", " +
                "\"name\": \"StringHolder\", " +
                "\"fields\": " +
                "[{\"name\":\"value\", \"type\": \"string\"}]}");


        ByteArrayOutputStream os = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(
                schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
                writer);
        try {
			dataFileWriter.create(schema, os);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        // Populate data
        GenericRecord datum = new GenericData.Record(schema);
        datum.put("value", (new org.apache.avro.util.Utf8(str)).toString());

        try {
			dataFileWriter.append(datum);
		     dataFileWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
   

        System.out.println("encoded string: " + os.toString());




            return os.toByteArray();	
        
    }

	public byte[] toBytes(Object arg0) {
		return this.toBytes(arg0.toString());
	}

}
