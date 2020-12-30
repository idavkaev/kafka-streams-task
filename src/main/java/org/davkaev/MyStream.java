package org.davkaev;

import ch.hsr.geohash.GeoHash;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.davkaev.domain.Address;
import org.davkaev.domain.Weather;
import org.davkaev.domain.WeatherAgg;
import org.davkaev.serdes.PojoDeserializer;
import org.davkaev.serdes.PojoSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class MyStream {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mystream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final String SOURCE_ADDRESSES_TOPIC = "addresses2";
        final String SOURCE_WEATHER_TOPIC = "weather_100";
        final String INTERMEDIATE_WEATHER_TOPIC_HASHDATE = "weather_hash_date";
        final String INTERMEDIATE_WEATHER_TOPIC_HASH = "weather_hash";

        final StreamsBuilder builder = new StreamsBuilder();

        ObjectMapper om = new ObjectMapper();

        final Serde<String> stringSerde = Serdes.String();
        final Serde<byte[]> byteArraySerde = Serdes.ByteArray();

        Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<Weather> weatherSerializer = new PojoSerializer<>();
        serdeProps.put("JsonPOJOClass", Weather.class);
        weatherSerializer.configure(serdeProps, false);

        final Deserializer<Weather> weatherDeserializer = new PojoDeserializer<>();
        serdeProps.put("JsonPOJOClass", Weather.class);
        weatherDeserializer.configure(serdeProps, false);
        final Serde<Weather> weatherSerde = Serdes.serdeFrom(weatherSerializer, weatherDeserializer);

        final Serializer<WeatherAgg> weatherStatsSerializer = new PojoSerializer<>();
        serdeProps.put("JsonPOJOClass", WeatherAgg.class);
        weatherStatsSerializer.configure(serdeProps, false);

        final Deserializer<WeatherAgg> weatherStatsDeserializer = new PojoDeserializer<>();
        serdeProps.put("JsonPOJOClass", WeatherAgg.class);
        weatherStatsDeserializer.configure(serdeProps, false);
        final Serde<WeatherAgg> weatherStatSerde = Serdes.serdeFrom(weatherStatsSerializer, weatherStatsDeserializer);

        final Serializer<Address> addressSerializer = new PojoSerializer<>();
        serdeProps.put("JsonPOJOClass", Address.class);
        addressSerializer.configure(serdeProps, false);

        final Deserializer<Address> addressDeserializer = new PojoDeserializer<>();
        serdeProps.put("JsonPOJOClass", Address.class);
        addressDeserializer.configure(serdeProps, false);
        final Serde<Address> addressSerde = Serdes.serdeFrom(addressSerializer, addressDeserializer);

        final KStream<byte[], String> addresses = builder.stream(SOURCE_ADDRESSES_TOPIC, Consumed.with(byteArraySerde, stringSerde));       // source addresses
        final KStream<String, String> weathers = builder.stream(SOURCE_WEATHER_TOPIC, Consumed.with(stringSerde, stringSerde));             // source weather
        final KStream<String, Weather> w1 = builder.stream(INTERMEDIATE_WEATHER_TOPIC_HASHDATE, Consumed.with(stringSerde, weatherSerde));  // intermediate weather (by hash+date)
        final KStream<String, Weather> w2 = builder.stream(INTERMEDIATE_WEATHER_TOPIC_HASH, Consumed.with(stringSerde, weatherSerde));      // intermediate weather (by hash)
//        final KStream<String, WeatherAgg> w3 = builder.stream("weather_100_tr", Consumed.with(stringSerde, weatherStatSerde));            // final weather aggregated
        final KTable<String, Address> addressKTable;

        /*
         * Mapping weather by double key (date + hash)
         */
        weathers
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
                                new Weather(node.get("avg_tmpr_f").doubleValue(), node.get("avg_tmpr_c").doubleValue()));

                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    return null;
                }).to(INTERMEDIATE_WEATHER_TOPIC_HASHDATE, Produced.with(stringSerde, weatherSerde));

        /*
         * Mapping weather by hash
         */
        w1.groupByKey()
                .aggregate(WeatherAgg::new,
                        (key, value, agg) -> {
                            agg.addWeather(value);
                            return agg;
                        },
                        Materialized.with(stringSerde, weatherStatSerde))
                .toStream()
                .map((s, weatherStat) -> {
                    String[] keyParts = s.split("_");
                    Weather avgWeather = weatherStat.avgTmp();
                    avgWeather.setDate(keyParts[1]);
                    return KeyValue.pair(keyParts[0], avgWeather);
                })
                .to(INTERMEDIATE_WEATHER_TOPIC_HASH, Produced.with(stringSerde, weatherSerde));

        /*
         * Aggregating weather
         */
//        w2.groupByKey()
//                .aggregate(
//                        WeatherAgg::new,
//                        (key, value, agg) -> {
//                            agg.addWeather(value);
//                            return agg;
//                        },
//                        Materialized.with(stringSerde, weatherStatSerde))
//                .toStream()
//                .to("weather_100_tr", Produced.with(stringSerde, weatherStatSerde));

        /*
         * Addresses to table
         */
        addressKTable = addresses
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
                }).toTable(Materialized.with(stringSerde, addressSerde));

        addressKTable.leftJoin(w2.toTable(), (address, weather) -> {
            if (weather != null) {
                address.addWeather(weather);
            }
            return address;
        })
                .toStream()
                .to("aw", Produced.with(stringSerde, addressSerde));

        /*
         * joining addresses and weather
         */
//        addressKTable
//                .leftJoin(weatherKTable, (address, weather) -> {
//                    System.out.println("A: " + address);
//                    System.out.println("W: " + weather);
//
//                    if (weather != null) {
//                        address.addWeather(weather.getWeatherList());
//                    }
//                    return address;
//                })
//                .toStream()
//                .to("aw", Produced.with(stringSerde, addressSerde));

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);

        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
