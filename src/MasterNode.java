import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
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
    private final long startTime;

    public MasterNode(String[] files) {
        this.files = files;
        this.workers = Config.loadWorkers();
        this.finalResults = new ConcurrentHashMap<>();
        this.commHandler = new CommunicationHandler(Config.MASTER.port, this::handleMessage, workers);
        this.minMaxReports = ConcurrentHashMap.newKeySet();
        this.localMins = Collections.synchronizedList(new ArrayList<>());
        this.localMaxs = Collections.synchronizedList(new ArrayList<>());
        this.startTime = System.currentTimeMillis();

    }

    public void start() throws IOException {
        commHandler.start();
        Config.consoleOutput(Config.outType.INFO, "Master started. Assigning tasks...");
        assignFilesToWorkers("NEW");
        waitForTaskCompletion();
        Config.consoleOutput(Config.outType.INFO, "All tasks completed. Initiating reduction...");
        startReduce();
        waitForMinMaxReports();
        Config.consoleOutput(Config.outType.INFO, "All min/max received. Initiating redistribution...");
        redistributeByCounts();
        waitForRedistributionDone();
        Config.consoleOutput(Config.outType.INFO, "All redistribution done. Requesting for final results...");
        requestFinalResults();
        gatherFinalResults();
    }

    private void assignFilesToWorkers() throws IOException {
        // Old version: assign one file to one worker
        taskLatch = new CountDownLatch(files.length); // One count per task

        for (int i = 0; i < files.length; i++) {
            NodeInfo worker = workers.get(i % workers.size());
            String content = Files.readString(Paths.get(files[i]));
            commHandler.send(worker, new Message(Message.Type.TASK_ASSIGNMENT, content, -1));
        }
    }
    private void assignFilesToWorkers(String version) throws IOException {
        // New version: split every file into several parts matching the # of workers
        // This would allow the master to split task equally and automatically for the workers
        int numParts = workers.size();
        taskLatch = new CountDownLatch(files.length * numParts); // One count per task
        for(String filePath : files)
        {
            String content = Files.readString(Path.of(filePath));
            int totalLength = content.length();
            int partSize = totalLength / numParts;

            List<String> parts = new ArrayList<>(numParts);
            for (int i = 0; i < numParts; i++) {
                int start = i * partSize;
                int end = (i == numParts - 1) ? totalLength : (i + 1) * partSize;
                NodeInfo worker = workers.get(i);
                String task = content.substring(start, end);
                commHandler.send(worker, new Message(Message.Type.TASK_ASSIGNMENT, task, -1));
            }
        }
    }

    private void startReduce(){
        taskLatch = new CountDownLatch(workers.size());
        broadcast(new Message(Message.Type.START_REDUCE, "", -1));
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
                synchronized (lock) {
                    taskLatch.countDown();
                    Config.consoleOutput(Config.outType.INFO, "Received min/max from worker " + msg.senderId);
                }
                String[] parts = msg.payload.split(",");
                localMins.add(Integer.parseInt(parts[0]));
                localMaxs.add(Integer.parseInt(parts[1]));
                minMaxReports.add(String.valueOf(msg.senderId));
            }
            case REDISTRIBUTION_DONE -> {
                synchronized (lock) {
                    taskLatch.countDown();
                    Config.consoleOutput(Config.outType.INFO, "Redistribution done from worker " + msg.senderId);
                }
            }
            case FINAL_RESULT -> {
                int id = Integer.parseInt(msg.payload.split(":")[0]);
                String data = msg.payload.substring(msg.payload.indexOf(":") + 1);
                synchronized (lock) {
                    taskLatch.countDown();
                    Config.consoleOutput(Config.outType.INFO, "Received final result from worker " + id);
                }
                finalResults.put(id, data);
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
        try {
            taskLatch.await(); // Waits until all tasks are marked done
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for task completion");
        }
    }

    private void redistributeByCounts() {
        int globalMin = Collections.min(localMins);
        int globalMax = Collections.max(localMaxs);
        List<Integer> thresholds = new ArrayList<>();

        // Linear distribution interval
        /*
        int range = globalMax - globalMin;
        int interval = range / workers.size();

        for (int i = 1; i < workers.size(); i++) {
            thresholds.add(globalMin + i * interval);
        }
        */

        // Logarithmic distribution interval
        double minLog = Math.log(globalMin == 0 ? 1 : globalMin);
        double maxLog = Math.log(globalMax);
        double alpha = 10.0;

        for (int i = 1; i < workers.size(); i++) {
            double ratio = Math.pow((double) i / workers.size(), alpha);  // apply steepness
            double logThreshold = minLog + ratio * (maxLog - minLog);
            int threshold = (int) Math.round(Math.exp(logThreshold));
            thresholds.add(threshold);
        }

        String payload = String.join(",", thresholds.stream().map(Object::toString).toArray(String[]::new));
        taskLatch = new CountDownLatch(workers.size());
        broadcast(new Message(Message.Type.START_REDISTRIBUTE, payload, -1));
    }

    private void waitForRedistributionDone() {
        try {
            taskLatch.await(); // Waits until all tasks are marked done
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for task completion");
        }
    }

    private void requestFinalResults(){
        taskLatch = new CountDownLatch(workers.size());
        broadcast(new Message(Message.Type.SORT_AND_SEND_RESULT,
                "KEEP_LOCAL", -1));
        // Tell the slave to send result explicitly or keep result locally
        // use "EXPLICIT" or "KEEP_LOCAL" as control string
    }

    private void gatherFinalResults() {
        try {
            taskLatch.await(); // Waits until all tasks are marked done
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for task completion");
        }
        List<Integer> ids = new ArrayList<>(finalResults.keySet());
        Collections.sort(ids);
        long endTime = System.currentTimeMillis();
        long durationNano = endTime - startTime;
        double durationSeconds = durationNano / 1000.0;
        Config.consoleOutput(Config.outType.INFO, "Total running time: " + durationSeconds + "s.");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("final_result.txt"))) {
            for (int id : ids) {
                writer.write("Node " + id + "\n");
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
