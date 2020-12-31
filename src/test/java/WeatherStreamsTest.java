import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.test.TestUtils;
import org.davkaev.WeatherHotelsApp;
import org.davkaev.domain.Address;
import org.davkaev.domain.Weather;
import org.davkaev.serdes.CustomSerdes;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

//TODO Add tests for each topology part
public class WeatherStreamsTest {

    private Properties streamAppProperties;

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
    public void testAggregateWeather() {
        StreamsBuilder builder = new StreamsBuilder();
        Topology topology = WeatherHotelsApp.getStreamingAppTopology(builder);
        TopologyTestDriver td = new TopologyTestDriver(topology, streamAppProperties);
        TestInputTopic<String, String> inputWeatherTopic = td.createInputTopic(WeatherHotelsApp.INPUT_TOPIC_WEATHER, Serdes.String().serializer(), Serdes.String().serializer());
        TestInputTopic<byte[], String> addressTopic = td.createInputTopic(WeatherHotelsApp.INPUT_TOPIC_HOTELS, Serdes.ByteArray().serializer(), Serdes.String().serializer());
        TestOutputTopic<String, Address> outputTopic = td.createOutputTopic(WeatherHotelsApp.OUTPUT_TOPIC, Serdes.String().deserializer(), CustomSerdes.getAddressSerde().deserializer());

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
