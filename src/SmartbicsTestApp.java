import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class SmartbicsTestApp {
    public static Path dir = Paths.get("D:/logs");
    public static Path filePath = Paths.get("D:/logs/Statistics.txt");
    public static ChronoUnit defaultUnit = ChronoUnit.HOURS;

    public static void main(String[] args) throws IOException {
        long startTime = System.currentTimeMillis();
        ChronoUnit unit = (args.length != 0) ? ChronoUnit.valueOf(args[0]) : defaultUnit;
        Map<LocalDateTime, AtomicInteger> dateCounterMap = new ConcurrentHashMap<>();
        Files.walk(dir)
                .filter(filePath -> filePath.toString().endsWith(".log"))
                .parallel()
                .flatMap(path -> {
                    try {
                        return Files.lines(path);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .filter(line -> line.contains("ERROR"))
                .map(line -> LocalDateTime.parse(line.split(";")[0]))
                .forEach(localDateTime -> {
                    LocalDateTime l = localDateTime.truncatedTo(unit);
                    AtomicInteger errorCounter = dateCounterMap.get(l);
                    if (errorCounter == null) {
                        dateCounterMap.put(l, new AtomicInteger(1));
                    } else {
                        errorCounter.incrementAndGet();
                    }
                });
        List<String> list = dateCounterMap.keySet().stream()
                .sorted()
                .map(date -> date.toLocalDate() +
                        "," +
                        " " +
                        date.toLocalTime().truncatedTo(unit) +
                        " - " +
                        date.plus(1, unit).toLocalTime() +
                        " Количество ошибок: " +
                        dateCounterMap.get(date)).collect(Collectors.toList());
        Files.write(filePath, list, StandardCharsets.UTF_8);
        System.out.println(System.currentTimeMillis() - startTime);
    }

}