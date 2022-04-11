package roshult.flink.training.consumer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class TaxiEventDeserializer implements Deserializer<TaxiRide> {
    ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public void configure(Map configs, boolean isKey) {}

    @Override
    public TaxiRide deserialize(String topic, byte[] data) {
        TaxiRide ride = null;
        try {
            ride = objectMapper.readValue(data, TaxiRide.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ride;
    }

    @Override
    public void close() {}
}
