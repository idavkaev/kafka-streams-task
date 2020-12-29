package org.davkaev.domain;

import lombok.Data;

import java.util.LinkedList;
import java.util.List;

@Data
public class WeatherAgg {
    List<Weather> weatherList;

    public WeatherAgg() {
        this.weatherList = new LinkedList<>();
    }

    public WeatherAgg addWeather(Weather weather) {
        this.weatherList.add(weather);
        return this;
    }

    public Weather avgTmp(){
        double avg_f = weatherList.stream().mapToDouble(Weather::getTmp_f).summaryStatistics().getAverage();
        double avg_c = weatherList.stream().mapToDouble(Weather::getTmp_c).summaryStatistics().getAverage();
        return new Weather(avg_f, avg_c);
    }
}
