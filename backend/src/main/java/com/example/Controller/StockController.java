package com.example.Controller;


import com.example.entity.Stock;
import com.example.Service.StockService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

        import java.net.URI;
import java.util.List;

@RestController
@RequestMapping("/api/stocks")
@CrossOrigin("*")
public class StockController {
    @Autowired
    private final StockService service;

    public StockController(StockService service) {
        this.service = service;
    }

    /** List all stocks or filter by sector / name */
    @GetMapping
    public List<Stock> list(
            @RequestParam(name = "sector", required = false) String sector,
            @RequestParam(name = "q", required = false) String nameQuery
    ) {
        if (sector != null && !sector.isBlank()) {
            return service.findBySector(sector);
        }
        if (nameQuery != null && !nameQuery.isBlank()) {
            return service.searchByName(nameQuery);
        }
        return service.listAll();
    }

    /** Get single stock by symbol */
    @GetMapping("/{symbol}")
    public Stock get(@PathVariable String symbol) {
        return service.getBySymbol(symbol);
    }

    /** Create a new stock (or update if same symbol exists) */
    @PostMapping
    public ResponseEntity<Stock> create(@Valid @RequestBody Stock stock) {
        Stock saved = service.createOrUpdate(stock);
        return ResponseEntity.created(URI.create("/api/stocks/" + saved.getSymbol())).body(saved);
    }

    /** Update an existing stock */
    @PutMapping("/{symbol}")
    public Stock update(@PathVariable String symbol, @Valid @RequestBody Stock stock) {
        stock.setSymbol(symbol); // ensure path param is the source of truth
        return service.createOrUpdate(stock);
    }

    /** Delete by symbol */
    @DeleteMapping("/{symbol}")
    public ResponseEntity<Void> delete(@PathVariable String symbol) {
        service.delete(symbol);
        return ResponseEntity.noContent().build();
    }

    /** Utility endpoints */
    @GetMapping("/symbols")
    public List<String> symbols() {
        return service.getAllSymbols();
    }

    @GetMapping("/sectors")
    public List<String> sectors() {
        return service.getDistinctSectors();
    }
}