import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;
import org.davkaev.WeatherHotelsApp;
import org.davkaev.domain.Address;
import org.davkaev.domain.Weather;
import org.davkaev.domain.WeatherAgg;
import org.davkaev.serdes.CustomSerdes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class WeatherStreamsTest {

    private Properties streamAppProperties;
    private static final String INPUT_TOPIC = "input_test";
    private static final String OUTPUT_TOPIC = "output_topic";

    @Before
    public void setup() {
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "testWeather");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        streamAppProperties = props;
    }

    @Test
    public void testWeatherGrouping() {

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Weather> weatherInput = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), CustomSerdes.getWeatherSerde()));
        KTable<String, WeatherAgg> weatherOutput = WeatherHotelsApp.countAvgTempByDays(weatherInput);
        weatherOutput.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), CustomSerdes.getWeatherAggSerde()));

        Topology topology = builder.build();
        TopologyTestDriver td = new TopologyTestDriver(topology, streamAppProperties);

        TestInputTopic<String, String> inputTopic = td.createInputTopic(
                INPUT_TOPIC,
                new StringSerializer(),
                new StringSerializer()
        );

        TestOutputTopic<String, String> outputTopic = td.createOutputTopic(
                OUTPUT_TOPIC,
                new StringDeserializer(),
                new StringDeserializer()
        );

        inputTopic.pipeKeyValueList(List.of(
                KeyValue.pair("u09t_2016-10-31", "{\"tmp_f\":23.8,\"tmp_c\":-4.6,\"date\":\"2016-10-31\"}"),
                KeyValue.pair("gcpv_2016-10-01", "{\"tmp_f\":59.9,\"tmp_c\":15.5,\"date\":\"2016-10-01\"}"),
                KeyValue.pair("u09t_2016-10-26", "{\"tmp_f\":56.5,\"tmp_c\":13.6,\"date\":\"2016-10-26\"}")
        ));

        List<KeyValue<String, String>> expectedResult = List.of(
                KeyValue.pair("u09t", "{\"weatherList\":[{\"tmp_f\":23.8,\"tmp_c\":-4.6,\"date\":\"2016-10-31\"}],\"date\":\"2016-10-31\"}"),
                KeyValue.pair("gcpv", "{\"weatherList\":[{\"tmp_f\":59.9,\"tmp_c\":15.5,\"date\":\"2016-10-01\"}],\"date\":\"2016-10-01\"}"),
                KeyValue.pair("u09t", "{\"weatherList\":[{\"tmp_f\":23.8,\"tmp_c\":-4.6,\"date\":\"2016-10-31\"},{\"tmp_f\":56.5,\"tmp_c\":13.6,\"date\":\"2016-10-26\"}],\"date\":\"2016-10-31\"}")
        );

        Assert.assertFalse(outputTopic.isEmpty());
        Assert.assertEquals(expectedResult.size(), outputTopic.readKeyValuesToList().size());
        Assert.assertTrue(expectedResult.containsAll(outputTopic.readKeyValuesToList()));
    }

    @Test
    public void testHashAddresses() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<byte[], String> addressInput = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.ByteArray(), Serdes.String()));
        KStream<String, Address> addressOutput = WeatherHotelsApp.getAddressStream(addressInput);
        addressOutput.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), CustomSerdes.getAddressSerde()));

        Topology topology = builder.build();
        TopologyTestDriver td = new TopologyTestDriver(topology, streamAppProperties);

        TestInputTopic<String, String> inputTopic = td.createInputTopic(
                INPUT_TOPIC,
                new StringSerializer(),
                new StringSerializer()
        );
        TestOutputTopic<String, String> outputTopic = td.createOutputTopic(
                OUTPUT_TOPIC,
                new StringDeserializer(),
                new StringDeserializer()
        );

        inputTopic.pipeValueList(List.of(
                "{\"Address\":\"51 Gloucester Terrace Westminster Borough London W2 3DQ United Kingdom\"," +
                        "\"City\":\"Paddington\",\"Country\":\"GB\",\"Hash\":\"gcpv\",\"Id\":\"3401614098437\"," +
                        "\"Latitude\":\"51.5131074\",\"Longitude\":\"-0.1778707\",\"Name\":\"The Westbourne Hyde Park\"}",
                "{\"Address\":\"88 rue du Faubourg Poissoni re 10th arr 75010 Paris France\",\"City\":\"Paris\"," +
                        "\"Country\":\"FR\",\"Hash\":\"u09w\",\"Id\":\"3410204033026\",\"Latitude\":\"48.8776756\"," +
                        "\"Longitude\":\"2.3493159\",\"Name\":\"Best Western Premier Faubourg 88\"}",
                "{\"Address\":\"10 Rue de la Tour d Auvergne 9th arr 75009 Paris France\",\"City\":\"Paris\"," +
                        "\"Country\":\"FR\",\"Hash\":\"u09w\",\"Id\":\"3375844294657\",\"Latitude\":\"48.8789432\"," +
                        "\"Longitude\":\"2.3448623\",\"Name\":\"Hotel Tour d Auvergne Opera\"}",
                "{\"Address\":\"34 Rue de Buci 6th arr 75006 Paris France\",\"City\":\"Paris\",\"Country\":" +
                        "\"FR\",\"Hash\":\"u09t\",\"Id\":\"3358664425477\",\"Latitude\":\"48.8535639\",\"Longitude\":\"2.3360169\",\"Name\":\"Artus Hotel by MH\"}"
        ));

        List<KeyValue<String, String>> expectedResult = List.of(
                KeyValue.pair("gcpv", "{\"Address\":\"51 Gloucester Terrace Westminster Borough London W2 3DQ United Kingdom\"," +
                        "\"City\":\"Paddington\",\"Country\":\"GB\",\"Hash\":\"gcpv\",\"Id\":\"3401614098437\"," +
                        "\"Latitude\":\"51.5131074\",\"Longitude\":\"-0.1778707\",\"Name\":\"The Westbourne Hyde Park\"}"),
                KeyValue.pair("u09w", "{\"Address\":\"88 rue du Faubourg Poissoni re 10th arr 75010 Paris France\",\"City\":\"Paris\"," +
                        "\"Country\":\"FR\",\"Hash\":\"u09w\",\"Id\":\"3410204033026\",\"Latitude\":\"48.8776756\"," +
                        "\"Longitude\":\"2.3493159\",\"Name\":\"Best Western Premier Faubourg 88\"}"),
                KeyValue.pair("u09w", "{\"Address\":\"10 Rue de la Tour d Auvergne 9th arr 75009 Paris France\",\"City\":\"Paris\"," +
                        "\"Country\":\"FR\",\"Hash\":\"u09w\",\"Id\":\"3375844294657\",\"Latitude\":\"48.8789432\"," +
                        "\"Longitude\":\"2.3448623\",\"Name\":\"Hotel Tour d Auvergne Opera\"}"),
                KeyValue.pair("u09t", "{\"Address\":\"34 Rue de Buci 6th arr 75006 Paris France\",\"City\":\"Paris\",\"Country\":" +
                        "\"FR\",\"Hash\":\"u09t\",\"Id\":\"3358664425477\",\"Latitude\":\"48.8535639\",\"Longitude\":\"2.3360169\",\"Name\":\"Artus Hotel by MH\"}")
        );

        Assert.assertFalse(outputTopic.isEmpty());
        Assert.assertEquals(expectedResult.size(), outputTopic.readKeyValuesToList().size());
        Assert.assertTrue(expectedResult.containsAll(outputTopic.readKeyValuesToList()));
    }

    @Test
    public void testHashWeather() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> weatherInput = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, Weather> weatherOutput = WeatherHotelsApp.getHashDateWeatherStream(weatherInput);
        weatherOutput.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), CustomSerdes.getWeatherSerde()));

        Topology topology = builder.build();
        TopologyTestDriver td = new TopologyTestDriver(topology, streamAppProperties);

        TestInputTopic<String, String> inputTopic = td.createInputTopic(
                INPUT_TOPIC,
                new StringSerializer(),
                new StringSerializer()
        );
        TestOutputTopic<String, String> outputTopic = td.createOutputTopic(
                OUTPUT_TOPIC,
                new StringDeserializer(),
                new StringDeserializer()
        );

        inputTopic.pipeValueList(List.of(
                "{\"avg_tmpr_c\":19.8,\"avg_tmpr_f\":67.7,\"lat\":39.6467,\"lng\":-89.8455,\"wthr_date\":\"2017-08-29\"}",
                "{\"avg_tmpr_c\":16.5,\"avg_tmpr_f\":61.7,\"lat\":35.7395,\"lng\":-78.3249,\"wthr_date\":\"2016-10-31\"}",
                "{\"avg_tmpr_c\":10.9,\"avg_tmpr_f\":51.6,\"lat\":36.3367,\"lng\":-77.113,\"wthr_date\":\"2016-10-26\"}",
                "{\"avg_tmpr_c\":26.5,\"avg_tmpr_f\":79.7,\"lat\":39.2336,\"lng\":-108.67,\"wthr_date\":\"2017-08-29\"}",
                "{\"avg_tmpr_c\":17.4,\"avg_tmpr_f\":63.3,\"lat\":36.9639,\"lng\":-85.3242,\"wthr_date\":\"2016-10-26\"}"
        ));

        List<KeyValue<String, String>> expectedResult = List.of(
                KeyValue.pair("dp01_2017-08-29", "{\"tmp_f\":67.7,\"tmp_c\":19.8,\"date\":\"2017-08-29\"}"),
                KeyValue.pair("dq27_2016-10-31", "{\"tmp_f\":61.7,\"tmp_c\":16.5,\"date\":\"2016-10-31\"}"),
                KeyValue.pair("dq3n_2016-10-26", "{\"tmp_f\":51.6,\"tmp_c\":10.9,\"date\":\"2016-10-26\"}"),
                KeyValue.pair("9wfx_2017-08-29", "{\"tmp_f\":79.7,\"tmp_c\":26.5,\"date\":\"2017-08-29\"}"),
                KeyValue.pair("dne6_2016-10-26", "{\"tmp_f\":63.3,\"tmp_c\":17.4,\"date\":\"2016-10-26\"}")
        );

        Assert.assertFalse(outputTopic.isEmpty());
        Assert.assertEquals(expectedResult.size(), outputTopic.readKeyValuesToList().size());
        Assert.assertTrue(expectedResult.containsAll(outputTopic.readKeyValuesToList()));
    }

    @Test
    public void testAggregateWeather() {
        StreamsBuilder builder = new StreamsBuilder();
        Topology topology = WeatherHotelsApp.getStreamingAppTopology(builder);
        TopologyTestDriver td = new TopologyTestDriver(topology, streamAppProperties);

        TestInputTopic<String, String> inputWeatherTopic = td.createInputTopic(
                WeatherHotelsApp.INPUT_TOPIC_WEATHER,
                Serdes.String().serializer(),
                Serdes.String().serializer());

        TestInputTopic<byte[], String> addressTopic = td.createInputTopic(
                WeatherHotelsApp.INPUT_TOPIC_HOTELS,
                Serdes.ByteArray().serializer(),
                Serdes.String().serializer());

        TestOutputTopic<String, Address> outputTopic = td.createOutputTopic(
                WeatherHotelsApp.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                CustomSerdes.getAddressSerde().deserializer());

        inputWeatherTopic.pipeValueList(Arrays.asList(
                "{\"lat\":\"11111\", \"lng\":\"11111\", \"wthr_date\":\"2020-01-01\", \"avg_tmpr_f\": 70 , \"avg_tmpr_c\": 30 }",
                "{\"lat\":\"11111\", \"lng\":\"11111\", \"wthr_date\":\"2020-01-01\", \"avg_tmpr_f\": 72 , \"avg_tmpr_c\": 32 }",
                "{\"lat\":\"11111\", \"lng\":\"11111\", \"wthr_date\":\"2020-01-02\", \"avg_tmpr_f\": 72 , \"avg_tmpr_c\": 32 }"
        ));
        addressTopic.pipeValueList(Arrays.asList(
                "{\"Hash\":\"s000\", \"Country\": \"usa\", \"City\": \"1\", \"Id\": \"1\", \"Address\": \"1\", \"Name\": \"1\"}"
        ));

        Address addr1 = new Address("s000", "usa", "1", "1", "1", "1");
        addr1.setAvgWeathers(Arrays.asList(new Weather(71.0, 31.0, "2020-01-01"),
                new Weather(72.0, 32.0, "2020-01-02")));
        List<Address> expectedOut = Arrays.asList(addr1);

        assertThat(outputTopic.readValuesToList(), equalTo(expectedOut));
    }


}
