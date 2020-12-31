package org.davkaev.domain;

import lombok.Data;

import java.util.LinkedList;
import java.util.List;

@Data
public class WeatherAgg {
    List<Weather> weatherList;
    String date;

    public WeatherAgg() {
        this.weatherList = new LinkedList<>();
    }

    public WeatherAgg addWeather(Weather weather) {
        this.weatherList.add(weather);
        return this;
    }

    public WeatherAgg removeWeather(Weather weather) {
        this.weatherList.remove(weather);
        return this;
    }

    public String getDate() {
        if (weatherList.isEmpty()) {
            return null;
        } else {
            return weatherList.get(0).getDate();
        }
    }

    public Weather avgTmp(){
        double avg_f = weatherList.stream().mapToDouble(Weather::getTmp_f).summaryStatistics().getAverage();
        double avg_c = weatherList.stream().mapToDouble(Weather::getTmp_c).summaryStatistics().getAverage();
        return new Weather(avg_f, avg_c);
    }
}
