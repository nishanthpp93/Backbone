package org.ohnlp.backbone.io.local;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.ohnlp.backbone.api.Extract;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.util.LinkedList;
import java.util.List;

public class CSVExtract extends Extract {

    private String filePath;
    private String recordIDField;
    private String recordBodyField;
    private Schema rowSchema;

    private enum Header {
        id, text;
    }

    @Override
    public void initFromConfig(JsonNode config) throws ComponentInitializationException {
        this.filePath = config.get("fileSystemPath").asText();
        this.recordIDField = config.get("recordIDField").asText();
        this.recordBodyField = config.get("recordBodyField").asText();
        List<Schema.Field> fields = new LinkedList<>();
        fields.add(Schema.Field.of(recordIDField, Schema.FieldType.STRING));
        fields.add(Schema.Field.of(recordBodyField, Schema.FieldType.STRING));
        this.rowSchema = Schema.of(fields.toArray(new Schema.Field[0]));
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
        return input
                // .apply(TextIO.read().from(this.filePath))
                .apply(FileIO.match().filepattern(filePath)) //PCollection<Metadata>
                .apply(FileIO.readMatches()) //PCollection<ReadableFile>
                .apply(ParDo.of(
                        new DoFn<ReadableFile, CSVRecord>() {
                            @ProcessElement
                            public void processElement(@Element ReadableFile element, OutputReceiver<CSVRecord> receiver) throws IOException {
                                InputStream is = Channels.newInputStream(element.open());
                                Reader reader = new InputStreamReader(is, "UTF-8");
                                Iterable<CSVRecord> records = CSVFormat.DEFAULT.withHeader(Header.class).withDelimiter(',').withFirstRecordAsHeader().parse(reader);
                                for (CSVRecord record : records) { receiver.output(record); }
                            }}
                ))
                .apply("Reshuffle Records", Reshuffle.viaRandomKey())
                .apply(ParDo.of(new DoFn<CSVRecord, Row>() {
                    @ProcessElement
                    public void processElement(@Element CSVRecord input, OutputReceiver<Row> output) throws IOException {
                        String id = input.get(0);
                        String content = input.get(1);
                        output.output(Row.withSchema(rowSchema)
                                .withFieldValue(recordIDField, id)
                                .withFieldValue(recordBodyField, content)
                                .build());
                    }}
                )).setRowSchema(rowSchema);
    }
}
