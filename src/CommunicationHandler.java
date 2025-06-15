import java.io.*;
import java.net.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

public class CommunicationHandler {
    private final int port;
    private final BiConsumer<Message, String> onMessage;
    private final List<NodeInfo> peers; // All known peers (workers + master, excluding self)
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private final Map<String, ObjectOutputStream> outStreams = new ConcurrentHashMap<>();

    public CommunicationHandler(int port, BiConsumer<Message, String> onMessage, List<NodeInfo> peers) {
        this.port = port;
        this.onMessage = onMessage;
        this.peers = peers;
    }

    /*public void start() {
        pool.submit(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                while (true) {
                    Socket client = serverSocket.accept();
                    pool.submit(() -> handle(client));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private void handle(Socket client) {
        try (ObjectInputStream in = new ObjectInputStream(client.getInputStream())) {
            Message msg = (Message) in.readObject();
            String senderHost = client.getInetAddress().getHostAddress();
            String senderPort = Integer.toString(client.getPort());
            System.out.println("Received message: " + msg.type + " from " + senderHost + ":" + senderPort);
            onMessage.accept(msg, senderHost);
        } catch (Exception e) {
            System.err.println("Error handling client at " + client.getInetAddress());
            e.printStackTrace();
        }
    }

    public void send(NodeInfo worker, Message msg) {
        try (Socket socket = new Socket(worker.hostname, worker.port);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
            out.writeObject(msg);
            out.flush(); // ✅ flush before closing
        } catch (IOException e) {
            System.err.println("Failed to send to " + worker.hostname + ":" + worker.port);
            e.printStackTrace();
        }
    }*/

    public void start() {
        startServerSocket();   // Accept inbound connections
        connectToPeers();      // Establish outbound connections
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
                String senderPort = Integer.toString(client.getPort());
                Config.consoleOutput(Config.outType.DEBUG,"Received message: " + msg.type + " from " + sender + ":" + senderPort);
                onMessage.accept(msg, sender);
            }
        } catch (Exception e) {
            Config.consoleOutput(Config.outType.ERR, "Error handling client at " + client.getInetAddress());
            e.printStackTrace();
        }
    }

    private void connectToPeers() {
        for (NodeInfo peer : peers) {
            pool.submit(() -> {
                while (true) {
                    try {
                        Socket socket = new Socket(peer.hostname, peer.port);
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        outStreams.put(peer.hostname + ":" + peer.port, out);

                        // Optional: start a listener for server-to-client messages if bidirectional
                        return;
                    } catch (IOException e) {
                        System.err.println("Retrying connection to " + peer.hostname + ":" + peer.port);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ignored) {}
                    }
                }
            });
        }
    }

    public void send(NodeInfo peer, Message msg) {
        String key = peer.hostname + ":" + peer.port;
        ObjectOutputStream out = outStreams.get(key);
        if (out != null) {
            try {
                synchronized (out) {
                    Config.consoleOutput(Config.outType.DEBUG, msg.type + " sent to " + key);
                    out.writeObject(msg);
                    out.flush();
                }
            } catch (IOException e) {
                Config.consoleOutput(Config.outType.ERR, "Send failed to " + key);
                e.printStackTrace();
            }
        } else {
            Config.consoleOutput(Config.outType.ERR, "No connection to " + key);
        }
    }

}