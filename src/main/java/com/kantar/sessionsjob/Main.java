package com.kantar.sessionsjob;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main {

    // See README.txt for usage example

    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Missing arguments: <input-statements-file> <output-sessions-file>");
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
     * @param args
     * @throws IOException
     */
    public static void processSession(String[] args) throws IOException {
        String inputFilename = args[0];//input-statements-file.psv
        String outputFilename = args[1];//actual-sessions.psv
        int numberOfThreads = Runtime.getRuntime().availableProcessors(); // Use available CPU cores
        List<Session> dataList = Collections.synchronizedList(new ArrayList<>()); // Thread-safe list
        Map<String, List<Session>> mapOfHomeNoAndSession = new HashMap<>();
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        boolean isProceedForFileGeneratiom = true;
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilename))) {
            String line;
            String headerLine = reader.readLine();
            while ((line = reader.readLine()) != null) {
                final String currentLine = line; // Effectively final for lambda
                executor.submit(() -> {
                    try {
                        // Simulate parsing and processing a line
                        String[] tokens = currentLine.split("\\|"); // Assuming comma-separated values
                        Session session = new Session();
                        session.setHomeNo(tokens[0]);
                        session.setChannel(tokens[1]);
                        session.setStartTime(tokens[2]);
                        session.setActivity(tokens[3]);
                        dataList.add(session);
                    } catch (Exception e) {
                        System.err.println("Error parsing line: " + currentLine + " - " + e.getMessage());
                    }
                });
            }
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        } finally {
            executor.shutdown();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                isProceedForFileGeneratiom = false;
                throw new RuntimeException(e);
            }
        }

        if(isProceedForFileGeneratiom) {
            //Sort on homeNo & StartTime
            Comparator<Session> multiFieldComparator = Comparator.comparing(Session::getHomeNo)
                    .thenComparing(Comparator.comparing(Session::getStartTime));
            dataList.sort(multiFieldComparator);
            mapOfHomeNoAndSession = dataList.stream()
                    .collect(Collectors.groupingBy(Session::getHomeNo, LinkedHashMap::new, Collectors.toList()));
            StringBuilder sb = new StringBuilder();
            List<String> outputFileLines = new ArrayList<>();
            processRecords(mapOfHomeNoAndSession, outputFilename);
        }
    }

    /**
     * Read input file & process data for output file
     * @param mapOfHomeNoAndSession
     * @param outputFilename
     * @throws IOException
     */
    private static void processRecords(Map<String, List<Session>> mapOfHomeNoAndSession, String outputFilename) throws IOException {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();

        try {
            String header = "HomeNo|Channel|Starttime|Activity|EndTime|Duration";
            queue.put(header);
            ExecutorService readerPool = Executors.newFixedThreadPool(mapOfHomeNoAndSession.size());
            readerPool.submit(() -> {
                mapOfHomeNoAndSession.forEach((home, sessionList) -> {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
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
                        try {
                            queue.put(outputLine);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });
                });
                try {
                    queue.put("EOF");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            });
            try {
                readerPool.shutdown();
                readerPool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            ExecutorService writerPool = Executors.newFixedThreadPool(mapOfHomeNoAndSession.size());
            try {
                //Delete file if created in target
                Files.deleteIfExists(Paths.get(outputFilename));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            writerPool.submit(() -> {
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilename, true))) {
                    while (true) {
                        String line = queue.take();
                        if ("EOF".equals(line)) break;
                        writer.write(line);
                        writer.newLine();
                        writer.flush();
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
            try {
                writerPool.shutdown();
                writerPool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //Just for output file validation purpose added below lines. So no exception handling added here
        System.out.println("Output psv file generated at:"+ outputFilename + " & File Content as below:");
        Files.lines(Paths.get(outputFilename)).forEach(s -> System.out.println(s));
    }
}
