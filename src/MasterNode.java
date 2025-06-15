import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

public class MasterNode {
    private final String[] files;
    private final List<NodeInfo> workers;
    private final Map<Integer, String> finalResults;
    private final CommunicationHandler commHandler;
    private final Set<String> minMaxReports;
    private final List<Integer> localMins;
    private final List<Integer> localMaxs;
    private int redisDoneCount = 0;
    private CountDownLatch taskLatch;
    private final Object lock = new Object(); // for printing/debug sync

    public MasterNode(String[] files) {
        this.files = files;
        this.workers = Config.loadWorkers();
        this.finalResults = new ConcurrentHashMap<>();
        this.commHandler = new CommunicationHandler(Config.MASTER_PORT, this::handleMessage, workers);
        this.minMaxReports = ConcurrentHashMap.newKeySet();
        this.localMins = Collections.synchronizedList(new ArrayList<>());
        this.localMaxs = Collections.synchronizedList(new ArrayList<>());
    }

    public void start() throws IOException {
        commHandler.start();
        Config.consoleOutput(Config.outType.INFO, "Master started. Assigning tasks...");
        assignFilesToWorkers();
        waitForTaskCompletion();
        Config.consoleOutput(Config.outType.INFO, "All tasks completed. Initiating reduction...");
        broadcast(new Message(Message.Type.START_REDUCE, "", -1));
        waitForMinMaxReports();
        redistributeByCounts();
        waitForRedistributionDone();
        broadcast(new Message(Message.Type.SORT_AND_SEND_RESULT, "", -1));
        gatherFinalResults();
    }

    private void assignFilesToWorkers() throws IOException {
        taskLatch = new CountDownLatch(files.length); // One count per task

        for (int i = 0; i < files.length; i++) {
            NodeInfo worker = workers.get(i % workers.size());
            String content = Files.readString(Paths.get(files[i]));
            commHandler.send(worker, new Message(Message.Type.TASK_ASSIGNMENT, content, -1));
        }
    }

    private void handleMessage(Message msg, String senderHost) {
        switch (msg.type) {
            case TASK_DONE -> {
                synchronized (lock) {
                    taskLatch.countDown();
                    Config.consoleOutput(Config.outType.INFO, "One task marked done. Remaining: " + taskLatch.getCount());
                }
            }
            case LOCAL_MIN_MAX -> {
                String[] parts = msg.payload.split(",");
                localMins.add(Integer.parseInt(parts[0]));
                localMaxs.add(Integer.parseInt(parts[1]));
                minMaxReports.add(String.valueOf(msg.senderId));
                Config.consoleOutput(Config.outType.INFO, "Received min/max from worker " + msg.senderId);
            }
            case REDISTRIBUTION_DONE -> {
                redisDoneCount++;
                Config.consoleOutput(Config.outType.INFO, "Redistribution done from " + senderHost);
            }
            case FINAL_RESULT -> {
                int id = Integer.parseInt(msg.payload.split(":")[0]);
                String data = msg.payload.substring(msg.payload.indexOf(":") + 1);
                finalResults.put(id, data);
                Config.consoleOutput(Config.outType.INFO, "Received final result from worker " + id);
            }
        }
    }

    private void waitForTaskCompletion() {
        try {
            taskLatch.await(); // Waits until all tasks are marked done
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for task completion");
        }
    }

    private void waitForMinMaxReports() {
        while (minMaxReports.size() < workers.size()) {
            sleep(100);
        }
    }

    private void redistributeByCounts() {
        int globalMin = Collections.min(localMins);
        int globalMax = Collections.max(localMaxs);
        int range = globalMax - globalMin;
        int interval = range / workers.size();
        List<Integer> thresholds = new ArrayList<>();
        for (int i = 1; i < workers.size(); i++) {
            thresholds.add(globalMin + i * interval);
        }
        String payload = String.join(",", thresholds.stream().map(Object::toString).toArray(String[]::new));
        broadcast(new Message(Message.Type.START_REDISTRIBUTE, payload, -1));
    }

    private void waitForRedistributionDone() {
        while (redisDoneCount < workers.size()) {
            sleep(100);
        }
    }

    private void gatherFinalResults() {
        while (finalResults.size() < workers.size()) {
            sleep(100);
        }
        List<Integer> ids = new ArrayList<>(finalResults.keySet());
        Collections.sort(ids);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("final_result.txt"))) {
            for (int id : ids) {
                writer.write(id + "\n");
                writer.write(finalResults.get(id));
                writer.newLine();
            }
            Config.consoleOutput(Config.outType.INFO, "Final result written to final_result.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void broadcast(Message msg) {
        for (NodeInfo w : workers) {
            commHandler.send(w, msg);
        }
    }

    private void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
        }
    }

    /*private void debug(String msg) {
        if (Config.DEBUG) {
            System.out.println("[DEBUG] " + msg);
        }
    }*/
}
