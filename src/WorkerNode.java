import java.util.*;
import java.util.concurrent.*;

public class WorkerNode {
    private final int id;
    private final NodeInfo masterNode;
    private final List<NodeInfo> peers;
    private final CommunicationHandler commHandler;
    private final Map<String, Integer> localCounts = new ConcurrentHashMap<>();
    private final List<WordPair> receivedPairs = Collections.synchronizedList(new ArrayList<>());
    private final List<WordPair> redistributedPairs = Collections.synchronizedList(new ArrayList<>());
    private CountDownLatch ACK_Latch;
    private final Object lock = new Object();

    public WorkerNode(int id) {
        this.id = id;
        this.masterNode = Config.MASTER;
        this.peers = Config.loadWorkers();
        List<NodeInfo> allOtherNodes = new ArrayList<>(peers);
        allOtherNodes.add(masterNode);
        this.commHandler = new CommunicationHandler(peers.get(id).port, this::handleMessage, allOtherNodes);
    }

    public void start() {
        commHandler.start();
        Config.consoleOutput(Config.outType.WARN, "Worker " + id + " started.");
    }

    private void handleMessage(Message msg, String senderHost) {
        switch (msg.type) {
            case TASK_ASSIGNMENT -> handleTask(msg.payload);
            case WORD_PAIR, REDISTRIBUTION -> onReceivingPair(msg);
            case REQ_ACK -> replyACK(msg);
            case ACK -> rcvACK();
            case START_REDUCE -> performReduction();
            case START_REDISTRIBUTE -> redistribute(msg.payload);
            case SORT_AND_SEND_RESULT -> sendFinalResult(msg.payload);
        }
    }

    private void handleTask(String text) {
        Config.consoleOutput(Config.outType.INFO, "Worker " + id + " received a task.");
        String[] words = text.trim().split("\\s+");
        for (String word : words) {
            String cleaned = word.toLowerCase().replaceAll("\\W", "");
            if (!cleaned.isEmpty()) {
                int targetWorker = Math.abs(cleaned.hashCode()) % peers.size();
                WordPair wp = new WordPair(cleaned, 1);
                if (targetWorker == this.id) {
                    // Instead of sending to self, store directly
                    receivedPairs.add(wp);
                } else {
                    sendToPeer(targetWorker, wp);
                }
            }
        }
        askAndWait4ACK();
        commHandler.send(masterNode, new Message(Message.Type.TASK_DONE, String.valueOf(id), id));
    }

    private void askAndWait4ACK(){
        for(NodeInfo peer : peers) {
            if(peer.id != this.id) {
                commHandler.send(peer, new Message(Message.Type.REQ_ACK, "", id));
                Config.consoleOutput(Config.outType.DEBUG, "Worker " + id + " has requested " + peer.id +" to ACK.");
            }
        }
        ACK_Latch = new CountDownLatch(peers.size()-1);
        Config.consoleOutput(Config.outType.INFO, "Worker " + id + " is waiting for peers ACKs.");
        try {
            ACK_Latch.await(); // Waits until all tasks are marked done
            Config.consoleOutput(Config.outType.INFO, "Worker " + id + " has gathered all ACKs.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for task completion");
        }
    }

    private void replyACK(Message msg) {
        int senderId = msg.senderId;
        Config.consoleOutput(Config.outType.DEBUG, "Worker " + id + " has send an ACK to " + senderId + ".");
        commHandler.send(peers.get(senderId), new Message(Message.Type.ACK, "", id));
    }

    private void rcvACK() {
        Config.consoleOutput(Config.outType.DEBUG, "Worker " + id + " has received an ACK.");
        ACK_Latch.countDown();
    }

    private void sendToPeer(int peerId, WordPair wp) {
        Message m = new Message(Message.Type.WORD_PAIR, wp.word + ":" + wp.count, id);
        commHandler.send(peers.get(peerId), m);
    }

    private void onReceivingPair(Message msg) {
        String[] parts = msg.payload.split(":");
        String word = parts[0];
        int count = Integer.parseInt(parts[1]);
        if(msg.type == Message.Type.WORD_PAIR) {
            synchronized (lock){
                receivedPairs.add(new WordPair(word, count));
            }
        }
        else if (msg.type == Message.Type.REDISTRIBUTION){
            synchronized (lock){
                redistributedPairs.add(new WordPair(word, count));
            }
        }

    }

    private void performReduction() {
        /*
        try{
            Thread.sleep(500); // wait for the on-air messages
        } catch (InterruptedException e) {
            Config.consoleOutput(Config.outType.WARN, "Worker " + id + " interrupted.");
        }
        */
        for (WordPair wp : receivedPairs) {
            localCounts.merge(wp.word, wp.count, Integer::sum);
        }
        int localMin = localCounts.values().stream().min(Integer::compare).orElse(0);
        int localMax = localCounts.values().stream().max(Integer::compare).orElse(0);
        commHandler.send(masterNode,
                new Message(Message.Type.LOCAL_MIN_MAX, localMin + "," + localMax, id));
    }

    private void redistribute(String payload) {
        Config.consoleOutput(Config.outType.DEBUG, "Worker " + id + " redistributing...");
        String[] split = payload.split(",");
        List<Integer> thresholds = new ArrayList<>();
        for (String s : split)
            thresholds.add(Integer.parseInt(s));

        for (Map.Entry<String, Integer> entry : localCounts.entrySet()) {
            String word = entry.getKey();
            int count = entry.getValue();
            int destWorker = 0;
            while (destWorker < thresholds.size() && count > thresholds.get(destWorker)) {
                destWorker++;
            }
            Message m = new Message(Message.Type.REDISTRIBUTION,
                    word + ":" + count, id);
            commHandler.send(peers.get(destWorker), m);
        }
        askAndWait4ACK();
        commHandler.send(masterNode, new Message(Message.Type.REDISTRIBUTION_DONE, "", id));
    }

    private void sendFinalResult(String ctrl) {
        synchronized (lock) {
            redistributedPairs.sort(Comparator
                    .comparingInt((WordPair wp) -> wp.count)
                    .thenComparing(wp -> wp.word));
        }
        /*Map<String, Integer> reduced = new TreeMap<>();
        for (WordPair wp : redistributedPairs) {
            reduced.merge(wp.word, wp.count, Integer::sum);
        }

        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, Integer> entry : reduced.entrySet()) {
            result.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }*/
        if (Objects.equals(ctrl, "EXPLICIT")){
            StringBuilder result = new StringBuilder();
            for (WordPair wp : redistributedPairs) {
                result.append(wp.word).append(": ").append(wp.count).append("\n");
            }

            commHandler.send(masterNode,
                    new Message(Message.Type.FINAL_RESULT, id + ":" + result.toString(), id));
        }
        else if (Objects.equals(ctrl, "KEEP_LOCAL")){
            commHandler.send(masterNode,
                    new Message(Message.Type.FINAL_RESULT, id + ": result saved locally", id));
        }
        Config.consoleOutput(Config.outType.INFO, "Worker " + id + " sent final result.");

    }

    /*private void debug(String msg) {
        if (Config.DEBUG) {
            System.out.println("[Worker " + id + "] " + msg);
        }
    }*/

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java WorkerNode <worker_id>");
            System.exit(1);
        }
        int id = Integer.parseInt(args[0]);
        new WorkerNode(id).start();
    }
}