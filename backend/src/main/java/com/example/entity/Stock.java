package com.example.entity;


import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

@Entity
@Table(name = "stocks") // change to your actual table name (e.g., "stock")
public class Stock {

    @Id
    @Column(name = "symbol", nullable = false, length = 20)
    @Size(max = 20)
    @NotBlank
    private String symbol;

    @Column(name = "name", nullable = false, length = 120)
    @Size(max = 120)
    @NotBlank
    private String name;

    @Column(name = "sector", length = 60)
    @Size(max = 60)
    private String sector;

    // Optional fields if present in your DB:
    // @Column(name = "isin", length = 12)
    // private String isin;
    // @Column(name = "exchange", length = 10) // e.g., NSE or BSE
    // private String exchange;

    public Stock() {}

    public Stock(String symbol, String name, String sector) {
        this.symbol = symbol;
        this.name = name;
        this.sector = sector;
    }

    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getSector() { return sector; }
    public void setSector(String sector) { this.sector = sector; }
}