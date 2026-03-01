package com.example.Repository;


import com.example.entity.Stock;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Spring Data JPA repository for {@link Stock} master data.
 */
@Repository
public interface StockRepository extends JpaRepository<Stock, String> {

    /** Retrieve all stock symbols (used by price update scheduler) */
    @Query("SELECT s.symbol FROM Stock s")
    List<String> findAllSymbols();

    /** Find all stocks in a given sector */
    List<Stock> findBySector(String sector);

    /** Search stocks by name (case-insensitive partial match) */
    List<Stock> findByNameContainingIgnoreCase(String name);

    /** All distinct sectors */
    @Query("SELECT DISTINCT s.sector FROM Stock s WHERE s.sector IS NOT NULL")
    List<String> findDistinctSectors();

    /** Useful helpers */
    Optional<Stock> findBySymbol(String symbol);
    boolean existsBySymbol(String symbol);
}
