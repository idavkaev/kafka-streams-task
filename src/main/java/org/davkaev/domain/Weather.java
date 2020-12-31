package org.davkaev.domain;

import lombok.Data;

@Data
public class Weather {

    private double tmp_f;
    private double tmp_c;
    private String date;

    public Weather(double tmp_f, double tmp_c, String date) {
        this.tmp_f = tmp_f;
        this.tmp_c = tmp_c;
        this.date = date;
    }

    public Weather(double tmp_f, double tmp_c) {
        this.tmp_f = tmp_f;
        this.tmp_c = tmp_c;
    }

    public Weather() {
        this.tmp_f = 0;
        this.tmp_c = 0;
    }

    @Override
    public String toString() {
        return "{" +
                "tmp_f=" + tmp_f +
                ", tmp_c=" + tmp_c +
                '}';
    }

}