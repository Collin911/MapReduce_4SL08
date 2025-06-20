import java.io.*;
import java.net.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

public class CommunicationHandler {
    private final int port;
    private final BiConsumer<Message, String> onMessage;
    private final List<NodeInfo> peers;
    private final ExecutorService pool = Executors.newCachedThreadPool();

    private final Map<String, SenderThread> senders = new ConcurrentHashMap<>();

    public CommunicationHandler(int port, BiConsumer<Message, String> onMessage, List<NodeInfo> peers) {
        this.port = port;
        this.onMessage = onMessage;
        this.peers = peers;
    }

    public void start() {
        startServerSocket();
        for (NodeInfo peer : peers) {
            connectToPeer(peer);
        }
    }

    private void startServerSocket() {
        pool.submit(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                while (true) {
                    Socket client = serverSocket.accept();
                    pool.submit(() -> handleIncoming(client));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private void handleIncoming(Socket client) {
        try {
            ObjectInputStream in = new ObjectInputStream(client.getInputStream());
            while (true) {
                Message msg = (Message) in.readObject();
                String sender = client.getInetAddress().getHostAddress();
                onMessage.accept(msg, sender);
            }
        } catch (Exception e) {
            Config.consoleOutput(Config.outType.ERR, "Error handling client at " + client.getInetAddress());
            e.printStackTrace();
        }
    }

    private void connectToPeer(NodeInfo peer) {
        pool.submit(() -> {
            String key = peer.hostname + ":" + peer.port;
            while (true) {
                try {
                    Socket socket = new Socket(peer.hostname, peer.port);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

                    SenderThread sender = new SenderThread(peer, out, key);
                    senders.put(key, sender);
                    pool.submit(sender);
                    Config.consoleOutput(Config.outType.INFO, "Connected to " + key);
                    return;
                } catch (IOException e) {
                    Config.consoleOutput(Config.outType.ERR, "Retrying connection to " + key);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {}
                }
            }
        });
    }

    public void send(NodeInfo peer, Message msg) {
        String key = peer.hostname + ":" + peer.port;
        SenderThread sender = senders.get(key);
        if (sender != null) {
            sender.send(msg);
        } else {
            Config.consoleOutput(Config.outType.ERR, "No sender for " + key);
        }
    }

    private class SenderThread implements Runnable {
        private final NodeInfo peer;
        private final String peerKey;
        private final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
        private ObjectOutputStream out;

        public SenderThread(NodeInfo peer, ObjectOutputStream out, String peerKey) {
            this.peer = peer;
            this.out = out;
            this.peerKey = peerKey;
        }

        public void send(Message msg) {
            queue.offer(msg);
        }

        public void run() {
            while (true) {
                try {
                    Message msg = queue.take();
                    synchronized (out) {
                        out.writeObject(msg);
                        out.flush();
                    }
                    Config.consoleOutput(Config.outType.DEBUG, msg.type + " sent to " + peerKey);
                } catch (IOException | InterruptedException e) {
                    Config.consoleOutput(Config.outType.ERR, "Connection lost to " + peerKey);
                    e.printStackTrace();
                    break;
                }
            }

            // Retry connection
            connectToPeer(peer);
        }
    }
}