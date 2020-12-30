package org.davkaev.domain;

import lombok.Data;

import java.util.LinkedList;
import java.util.List;

@Data
public class Address {
    private String hash;
    private String country;
    private String city;
    private String address;
    private String name;
    private String id;
    private List<Weather> avgWeathers = new LinkedList<>();

    public Address(String hash, String country, String city, String address, String name, String id) {
        this.hash = hash;
        this.country = country;
        this.city = city;
        this.address = address;
        this.name = name;
        this.id = id;
        this.avgWeathers = new LinkedList<>();
    }

    public Address() {
    }

    public void addWeather(List<Weather> weathers){
        if (weathers != null) {
            this.avgWeathers.addAll(weathers);
        }
    }

    public void addWeather(Weather weather) {
        if(weather != null) {
            this.avgWeathers.add(weather);
        }
    }
}
