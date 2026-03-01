package com.example.Service;
import com.example.entity.Stock;
import com.example.Repository.StockRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class StockService {

    @Autowired
    private StockRepository repo;

    // CRUD
    public Stock createOrUpdate(Stock stock) {
        stock.setSymbol(stock.getSymbol().toUpperCase()); // normalize NSE symbols
        return repo.save(stock);
    }


    public List<Stock> listAll() {
        return repo.findAll();
    }


    public Stock getBySymbol(String symbol) {
        return repo.findById(symbol.toUpperCase())
                .orElseThrow(() -> new IllegalArgumentException("Stock not found: " + symbol));
    }

    public void delete(String symbol) {
        repo.deleteById(symbol.toUpperCase());
    }

    // Queries

    public List<String> getAllSymbols() {
        return repo.findAllSymbols();
    }


    public List<String> getDistinctSectors() {
        return repo.findDistinctSectors();
    }


    public List<Stock> findBySector(String sector) {
        return repo.findBySector(sector);
    }


    public List<Stock> searchByName(String q) {
        return repo.findByNameContainingIgnoreCase(q);
    }
}