package roshult.flink.training.publisher;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import roshult.flink.training.source.TaxiRide;
import roshult.flink.training.source.TaxiRideGenerator;

import java.util.Optional;

public class TaxiRidePublisher {

    private final SourceFunction<TaxiRide> source;
    private final KafkaSink<TaxiRide> sink;

    public TaxiRidePublisher(SourceFunction<TaxiRide> source, KafkaSink<TaxiRide> sink) {
        this.source = source;
        this.sink = sink;
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        KafkaSink<TaxiRide> sink =
                KafkaSink.<TaxiRide>builder()
                        .setBootstrapServers(
                                Optional.ofNullable(params.get("bootstrap-servers"))
                                        .orElse("localhost:9092"))
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic("taxi-event")
                                        .setKafkaValueSerializer(CustomSerializer.class)
                                        .build())
                        .build();
        TaxiRidePublisher job = new TaxiRidePublisher(new TaxiRideGenerator(), sink);
        job.execute();
    }

    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(source);
        rides.sinkTo(sink);

        // execute the pipeline and return the result
        return env.execute("Long Taxi Rides");
    }
}
