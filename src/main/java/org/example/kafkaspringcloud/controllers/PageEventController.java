package org.example.kafkaspringcloud.controllers;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.example.kafkaspringcloud.events.PageEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@RestController
public class PageEventController {

    @Autowired
    private StreamBridge streamBridge;

    @Autowired
    private InteractiveQueryService interactiveQueryService;

    @GetMapping("/publish")
    public PageEvent publish(@RequestParam String name, @RequestParam String topic) {
        PageEvent event = new PageEvent(name, Math.random() > 0.5 ? "U1" : "U2", new Date(),
                10 * new Random().nextInt(10000));
        streamBridge.send(topic, event);
        return event;
    }

    @GetMapping(value = "/analytics", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String, Long>> analytics() {
        return Flux.interval(Duration.ofSeconds(1))
                .map(seq -> {
                    Map<String, Long> map = new HashMap<>();
                    try {
                        ReadOnlyWindowStore<String, Long> stats = interactiveQueryService.getQueryableStore(
                                "count-store",
                                QueryableStoreTypes.windowStore());
                        Instant now = Instant.now();
                        Instant from = now.minusSeconds(30);
                        KeyValueIterator<Windowed<String>, Long> windowedLongKeyValueIterator = stats.fetchAll(from,
                                now);
                        while (windowedLongKeyValueIterator.hasNext()) {
                            KeyValue<Windowed<String>, Long> next = windowedLongKeyValueIterator.next();
                            map.put(next.key.key(), next.value);
                        }
                        windowedLongKeyValueIterator.close();
                    } catch (Exception e) {
                        // State store not ready yet, return empty map
                        System.out.println("State store not ready: " + e.getMessage());
                    }
                    return map;
                });
    }
}
