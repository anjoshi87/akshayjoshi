package com.kantar.sessionsjob;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main {

    // See README.txt for usage example
    private final static String header = "HomeNo|Channel|Starttime|Activity|EndTime|Duration";
    private final static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) {

        if (args.length < 2) {
            logger.error("Missing arguments: <input-statements-file> <output-sessions-file>");
            System.exit(1);
        }

        // TODO: write application ...
        try {
            processSession(args);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Process input Sessions file &  generate expected output sessions file
     *
     * @param args
     * @throws IOException
     */
    public static void processSession(String[] args) throws IOException {
        try {
            String inputFilename = args[0];//input-statements-file.psv
            String outputFilename = args[1];//actual-sessions.psv
            List<Session> dataList = Collections.synchronizedList(new ArrayList<>()); // Thread-safe list
            boolean inputDataCollected = true;
            try (BufferedReader reader = new BufferedReader(new FileReader(inputFilename))) {
                logger.info("Reading input File & Collecting Data...");
                String line;
                while ((line = reader.readLine()) != null) {
                    final String currentLine = line; // Effectively final for lambda
                    // Simulate parsing and processing a line
                    String[] tokens = currentLine.split("\\|"); // Assuming pipe-separated values
                    Session session = new Session();
                    session.setHomeNo(tokens[0]);
                    session.setChannel(tokens[1]);
                    session.setStartTime(tokens[2]);
                    session.setActivity(tokens[3]);
                    dataList.add(session);
                }
                logger.info("Input File Data Collected.");
            } catch (IOException e) {
                inputDataCollected = false;
            }

            if (inputDataCollected) {


                logger.info("Session Processing Started.");
                processRecords(dataList, outputFilename);
                //Just for output file validation purpose added below lines. So no exception handling added here
                logger.info("Output psv file generated at:" + outputFilename + " & File Content as below:");
                Files.lines(Paths.get(outputFilename)).forEach(s -> System.out.println(s));
                logger.info("Session Processing Completed.");
            }

        } catch (Exception e) {
            logger.error("Error reading & processing input file: ", e);
        }

    }

    /**
     * Read input file & process data for output file
     *
     * @param dataList
     * @param outputFilename
     */
    private static void processRecords(List<Session> dataList, String outputFilename) {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        //Sort on homeNo & StartTime
        Comparator<Session> multiFieldComparator = Comparator.comparing(Session::getHomeNo)
                .thenComparing(Session::getStartTime);
        dataList.sort(multiFieldComparator);
        Map<String, List<Session>> mapOfHomeNoAndSession = dataList.stream()
                .collect(Collectors.groupingBy(Session::getHomeNo, LinkedHashMap::new, Collectors.toList()));
        try {
            ExecutorService readerPool = Executors.newFixedThreadPool(mapOfHomeNoAndSession.size());
            AtomicInteger counter = new AtomicInteger(0);
            // Submit reader tasks
            mapOfHomeNoAndSession.forEach((home, sessionList) -> {
                readerPool.submit(() -> {
                    IntStream.rangeClosed(0, sessionList.size() - 1).forEach(i -> {
                        Session previous = sessionList.get(i);
                        LocalDateTime dateTimePrev = LocalDateTime.parse(previous.getStartTime(), formatter);
                        String outputLine = "";
                        if (i == sessionList.size() - 1) {
                            LocalDateTime endOfDay = dateTimePrev.toLocalDate().atTime(LocalTime.of(23, 59, 59));
                            Duration gap = Duration.between(dateTimePrev, endOfDay);
                            outputLine = previous.getHomeNo() + "|" + previous.getChannel() + "|" + previous.getStartTime() + "|" + previous.getActivity() + "|" + endOfDay.format(formatter) + "|" + gap.plusSeconds(1).getSeconds();

                        } else {
                            Session next = sessionList.get(i + 1);
                            LocalDateTime dateTimeNext = LocalDateTime.parse(next.getStartTime(), formatter);
                            Duration duration = Duration.between(dateTimePrev, dateTimeNext);
                            Duration gap = duration.minusSeconds(1);
                            outputLine = previous.getHomeNo() + "|" + previous.getChannel() + "|" + previous.getStartTime() + "|" + previous.getActivity() + "|" + dateTimePrev.plus(gap).format(formatter) + "|" + duration.getSeconds();
                        }
                        counter.incrementAndGet();
                        putInQueue(queue, outputLine);
                    });
                });
            });

            // Shutdown reader pool and wait for completion
            readerPool.shutdown();
            readerPool.awaitTermination(5, TimeUnit.SECONDS);

            //Writer to the file
            List<String> results = new ArrayList<>(queue);
            //Sort on homeNo & StartTime to write in expected order
            List<String> sortedList = results.stream()
                    .sorted(Comparator.comparing((String key) ->
                            key.split("|")[0]).thenComparing(key -> key.split("|")[2]))
                    .collect(Collectors.toList());

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilename, true))) {
                writer.write(header);
                writer.newLine();
                for (String entry : sortedList) {
                    writer.write(entry);
                    writer.newLine();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            logger.error("Exception occurred while processing & populaating output psv file.", e);
        }
    }

    /**
     * Put data in blocking queue
     *
     * @param queue
     * @param outputLine
     */
    private static void putInQueue(BlockingQueue<String> queue, String outputLine) {
        try {
            queue.put(outputLine);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
