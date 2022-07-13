package m.json2avro;

import cdr.types.Reading;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class MJsonToAvroProcessor extends AbstractProcessor {

    private FlowFile flowFile;

    // 1 - SETTINGS Success
    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("Success")
            .description("Meter file converted to Avro successfully")
            .build();

    // 2 - SETTINGS Failure
    public static final Relationship ERROR_RELATIONSHIP = new Relationship.Builder()
            .name("Failure")
            .description("Meter file processed with errors")
            .build();

    private Set<Relationship> relationships;

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS_RELATIONSHIP);
        relationships.add(ERROR_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        String fileName = flowFile.getAttribute("filename");
        long timestamp = new Timestamp(System.currentTimeMillis()).getTime();

        session.putAttribute(flowFile, "filename", fileName.substring(0, fileName.lastIndexOf('.')) + "_" + timestamp + ".avro");

        flowFile = session.write(flowFile, (inputStream, outputStream) -> {
            final DatumWriter<Reading> datumWriter = new SpecificDatumWriter<>(Reading.class);
            try (DataFileWriter<Reading> dataFileWriter = new DataFileWriter<>(datumWriter)) {
                Reading.Builder cdrBuilder = Reading.newBuilder();
                Reading meter = new Reading();

                JsonReader reader = new JsonReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
                reader.setLenient(true);
                Gson gson = new Gson();
                reader.beginArray();
                dataFileWriter.create(meter.getSchema(), outputStream);
                while (reader.hasNext()) {
                    Model model = gson.fromJson(reader, Model.class);
                    cdrBuilder.setLastUpdate(model.getEnd_datetime());
                    cdrBuilder.setMeterSerial(model.getDevice_id());
                    cdrBuilder.setReading(model.getReading());
                    meter = cdrBuilder.build();
                    dataFileWriter.append(meter);
                }
                reader.endArray();
                reader.close();
            } catch (IOException ex) {
                logger.error("\nPROCESSOR ERROR 1: " + ex.getMessage() + "\n");
                session.transfer(flowFile, ERROR_RELATIONSHIP);
                return;
            }

            logger.info("Successfully transfer file :)");
        });
        session.transfer(flowFile, SUCCESS_RELATIONSHIP);
    }
}
