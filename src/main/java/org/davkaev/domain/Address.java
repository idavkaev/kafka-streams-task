package org.davkaev.domain;

import lombok.Data;

import java.util.LinkedList;
import java.util.List;

@Data
public class Address {
    String hash;
    String country;
    String city;
    String address;
    String name;
    String id;
    List<Weather> avgWeathers = new LinkedList<>();

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
}
