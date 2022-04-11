package roshult.flink.training.consumer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

public class Detector {
    private final KafkaSource<TaxiRide> source;
    private final ParameterTool params;

    public Detector(KafkaSource<TaxiRide> source, ParameterTool params) {
        this.source = source;
        this.params = params;
    }

    public JobExecutionResult execute() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WatermarkStrategy<TaxiRide> watermarkStrategy =
                WatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner(
                                (ride, streamRecordTimestamp) -> ride.getEventTimeMillis());

        DataStream<TaxiRide> rides = env.fromSource(source, watermarkStrategy, "kafkaSource");

        rides.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(ride -> ride.rideId)
                .process(new AlertFunction())
                .addSink(
                        JdbcSink.sink(
                                "insert into taxi_alarm (rideId) values (?)",
                                (statement, id) -> {
                                    statement.setString(1, String.valueOf(id));
                                },
                                JdbcExecutionOptions.builder()
                                        .withBatchSize(5)
                                        .withBatchIntervalMs(200)
                                        .withMaxRetries(5)
                                        .build(),
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl(Optional.ofNullable(params.get("db-url"))
                                                .orElse("jdbc:postgresql://localhost:5432/postgres"))
                                        .withDriverName("org.postgresql.Driver")
                                        .withUsername("postgres")
                                        .withPassword("postgres")
                                        .build()));
        ;

        // execute the pipeline and return the result
        return env.execute("Long Taxi Rides");
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        createInitialTable(params);
        KafkaSource<TaxiRide> source =
                KafkaSource.<TaxiRide>builder()
                        .setBootstrapServers(
                                Optional.ofNullable(params.get("bootstrap-servers"))
                                        .orElse("localhost:9092"))
                        .setTopics("taxi-event")
                        .setGroupId("taxi-ride-alarm")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setDeserializer(
                                KafkaRecordDeserializationSchema.valueOnly(
                                        TaxiEventDeserializer.class))
                        .build();
        Detector job = new Detector(source, params);
        job.execute();
    }

    private static void createInitialTable(ParameterTool params) throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");
        Connection c =
                DriverManager.getConnection(
                        Optional.ofNullable(params.get("db-url"))
                                .orElse("jdbc:postgresql://localhost:5432/postgres"), "postgres", "postgres");
        Statement stmt = c.createStatement();
        stmt.execute("create table if not exists taxi_alarm(rideId varchar(255));");
        stmt.close();
        c.close();
    }

    @VisibleForTesting
    public static class AlertFunction extends KeyedProcessFunction<Long, TaxiRide, Long> {

        private transient ValueState<TaxiRide> ongoingRideState;

        @Override
        public void open(Configuration config) throws Exception {
            ongoingRideState =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("ride events", TaxiRide.class));
            ;
        }

        @Override
        public void processElement(TaxiRide ride, Context context, Collector<Long> out)
                throws Exception {
            long timeout = getTimeout(ride);
            if (ongoingRideState.value() != null) {
                if (exceedTwoHours(ride)) {
                    out.collect(ride.rideId);
                }
                clearState(context);
            } else {
                updateState(ride, context, timeout);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Long> out)
                throws Exception {
            out.collect(context.getCurrentKey());
            ongoingRideState.clear();
        }

        private void updateState(
                TaxiRide ride,
                KeyedProcessFunction<Long, TaxiRide, Long>.Context context,
                long timeout)
                throws IOException {
            context.timerService().registerEventTimeTimer(timeout);
            ongoingRideState.update(ride);
        }

        private void clearState(KeyedProcessFunction<Long, TaxiRide, Long>.Context context)
                throws IOException {
            context.timerService().deleteEventTimeTimer(getTimeout(ongoingRideState.value()));
            ongoingRideState.clear();
        }

        private boolean exceedTwoHours(TaxiRide ride) throws IOException {
            return Math.abs(
                            ChronoUnit.HOURS.between(
                                    ride.eventTime, ongoingRideState.value().eventTime))
                    >= 2;
        }

        private long getTimeout(TaxiRide ride) {
            return Instant.ofEpochMilli(ride.getEventTimeMillis())
                    .plus(2, ChronoUnit.HOURS)
                    .toEpochMilli();
        }
    }
}
