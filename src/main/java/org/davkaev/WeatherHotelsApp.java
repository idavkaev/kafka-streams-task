package org.davkaev;

import ch.hsr.geohash.GeoHash;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.davkaev.domain.Address;
import org.davkaev.domain.Weather;
import org.davkaev.domain.WeatherAgg;
import org.davkaev.serdes.CustomSerdes;

import java.util.Properties;

public class WeatherHotelsApp {

    static final StreamsBuilder builder = new StreamsBuilder();
    static final ObjectMapper om = new ObjectMapper();
    public static final String INPUT_TOPIC_WEATHER = "weather_01";
    public static final String INPUT_TOPIC_HOTELS = "addresses2";
    public static final String OUTPUT_TOPIC = "hotels-weather_fff";
    public static final String WEATHER_HASH_TOPIC = "weather_hash";


    public static void main(String[] args) throws Exception {
        Topology topology = getStreamingAppTopology(builder);
        final KafkaStreams streams = new KafkaStreams(topology, getStreamingAppProps());

        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static Topology getStreamingAppTopology(StreamsBuilder builder) {

        KStream<String, Weather> weatherStream = getHashDateWeatherStream(
                builder.stream(INPUT_TOPIC_WEATHER,
                        Consumed.with(Serdes.String(), Serdes.String())));

        weatherStream.to(WEATHER_HASH_TOPIC, Produced.with(Serdes.String(), CustomSerdes.getWeatherSerde()));

        KTable<String, WeatherAgg> aggregatedWeatherTable = countAvgTempByDays(
                builder.stream(WEATHER_HASH_TOPIC,
                        Consumed.with(Serdes.String(), CustomSerdes.getWeatherSerde())));

        KStream<String, Address> addressStream = getAddressStream(
                builder.stream(INPUT_TOPIC_HOTELS,
                        Consumed.with(Serdes.ByteArray(), Serdes.String())));

        KStream<String, Address> resultStream = getHotelsWithWeather(addressStream, aggregatedWeatherTable);

        resultStream.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), CustomSerdes.getAddressSerde()));
        return builder.build();
    }

    public static KStream<String, Weather> getHashDateWeatherStream(KStream<String, String> weather) {
        return weather
                .map((k, s) -> {
                    try {
                        JsonNode node = om.readTree(s);
                        String keyTemplate = "%s_%s";
                        String hash = GeoHash.geoHashStringWithCharacterPrecision(
                                node.get("lat").doubleValue(),
                                node.get("lng").doubleValue(),
                                4
                        );
                        return KeyValue.pair(
                                String.format(keyTemplate, hash, node.get("wthr_date").textValue()),
                                new Weather(node.get("avg_tmpr_f").doubleValue(), node.get("avg_tmpr_c").doubleValue(), node.get("wthr_date").textValue()));

                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    return null;
                });
    }


    public static KTable<String, WeatherAgg> countAvgTempByDays(KStream<String, Weather> weathers) {
        return weathers
                .groupByKey()
                .aggregate(WeatherAgg::new,
                        (key, value, agg) -> {
                            agg.addWeather(value);
                            return agg;
                        },
                        Materialized.with(Serdes.String(), CustomSerdes.getWeatherAggSerde()))
                .mapValues(w -> {
                    Weather weather = w.avgTmp();
                    weather.setDate(w.getDate());
                    return weather;
                })
                .groupBy((k, v) -> KeyValue.pair(k.split("_")[0], v), Grouped.with(Serdes.String(), CustomSerdes.getWeatherSerde()))
                .aggregate(() -> new WeatherAgg(),
                        (applicationId, value, aggValue) -> aggValue.addWeather(value),
                        (applicationId, value, aggValue) -> aggValue.removeWeather(value),
                        Materialized.with(Serdes.String(), CustomSerdes.getWeatherAggSerde()));
    }

    public static KStream<String, Address> getAddressStream(KStream<byte[], String> addresses) {
        return addresses
                .map((bytes, s) -> {
                    try {
                        JsonNode node = om.readTree(s);
                        return KeyValue.pair(
                                node.get("Hash").textValue(),
                                new Address(
                                        node.get("Hash").textValue(),
                                        node.get("Country").textValue(),
                                        node.get("City").textValue(),
                                        node.get("Address").textValue(),
                                        node.get("Name").textValue(),
                                        node.get("Id").textValue()
                                ));
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    return null;
                });
    }

    public static KStream<String, Address> getHotelsWithWeather(KStream<String, Address> addressKStream, KTable<String, WeatherAgg> weatherKTable) {
        return addressKStream
                .join(weatherKTable, (address, weather) -> {
                    if (weather != null) {
                        address.addWeather(weather.getWeatherList());
                    }
                    return address;
                }, Joined.with(Serdes.String(), CustomSerdes.getAddressSerde(), CustomSerdes.getWeatherAggSerde()));
    }

    private static Properties getStreamingAppProps() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mystream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }
}
