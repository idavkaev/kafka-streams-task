package org.davkaev.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.davkaev.domain.Address;
import org.davkaev.domain.Weather;
import org.davkaev.domain.WeatherAgg;

import java.util.HashMap;
import java.util.Map;

public class CustomSerdes {

    public static Serde<Weather> getWeatherSerde() {
        return getCustomSerde(Weather.class);
    }

    public static Serde<WeatherAgg> getWeatherAggSerde() {
        return getCustomSerde(WeatherAgg.class);
    }

    public static Serde<Address> getAddressSerde() {
        return getCustomSerde(Address.class);
    }

    private static <T> Serde<T> getCustomSerde(Class<T> serClass) {
        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("JsonPOJOClass", serClass);
        final Serializer<T> serializer = new WeatherSerializer<>();
        final Deserializer<T> deserializer = new WeatherDeserializer<>();
        serializer.configure(serdeProps, false);
        deserializer.configure(serdeProps, false);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
