package org.ohnlp.backbone.io.local;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.ToJson;

import org.joda.time.Duration;
import org.ohnlp.backbone.api.Load;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

/**
 * Reads in records to a file system directory.
 *
 * <b>Note:</b> This component is intended for debugging use in local mode only - no guarantees are made about
 * functionality in other environments
 * <p>
 * Expected configuration structure:
 * <pre>
 *     {
 *         "fileSystemPath": "path/to/output/dir/to/write/records"
 *     }
 * </pre>
 */
public class JSONLSingleLoad extends Load {

    private String filenamePrefix;
    private int maxOutputPartitionMinutes;
    private int outputPartitionMinutesDelay;
    private int outputPartitionLateness;
    private int batchSize;

    @Override
    public void initFromConfig(JsonNode config) throws ComponentInitializationException {
        this.filenamePrefix = config.get("fileSystemPath").asText() + "/part-";
        this.maxOutputPartitionMinutes = 60;
        this.outputPartitionMinutesDelay = 15;
        this.outputPartitionLateness = 5;
        this.batchSize = 50000;
    }

    @Override
    public PDone expand(PCollection<Row> input) {
        input.apply("Serialize to Json", ToJson.of())
                .apply("Windowing",
                        Window.<String>into(FixedWindows.of(Duration.standardMinutes(this.maxOutputPartitionMinutes)))
                                .triggering(Repeatedly.forever(
                                        AfterFirst.of(AfterPane.elementCountAtLeast(this.batchSize),
                                                AfterProcessingTime
                                                        .pastFirstElementInPane()
                                                        .plusDelayOf(Duration.standardMinutes(this.outputPartitionMinutesDelay)))))
                                .withAllowedLateness(Duration.standardSeconds(this.outputPartitionLateness))
                                .discardingFiredPanes())
                .apply(TextIO.write().to(this.filenamePrefix).withWindowedWrites().withNumShards(1).withSuffix(".jsonl"));
        return PDone.in(input.getPipeline());
    }
}
