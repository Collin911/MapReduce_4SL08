import java.io.*;
import java.net.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

public class CommunicationHandler {
    private final int port;
    private final BiConsumer<Message, String> onMessage;
    private final List<NodeInfo> peers; // All known peers (workers + master, excluding self)
    private final ExecutorService pool = Executors.newCachedThreadPool();

    public CommunicationHandler(int port, BiConsumer<Message, String> onMessage, List<NodeInfo> peers) {
        this.port = port;
        this.onMessage = onMessage;
        this.peers = peers;
    }

    public void start() {
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

    // Send to another worker
// Send to another worker
    public void send(NodeInfo worker, Message msg) {
        try (Socket socket = new Socket(worker.hostname, worker.port);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
            out.writeObject(msg);
            out.flush(); // ✅ flush before closing
        } catch (IOException e) {
            System.err.println("Failed to send to " + worker.hostname + ":" + worker.port);
            e.printStackTrace();
        }
    }

    // Send to a given host:port (e.g., to master)
    public void send(String dstHost, int dstPort, Message msg) {
        try (Socket socket = new Socket(dstHost, dstPort);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
            out.writeObject(msg);
            out.flush();
        } catch (IOException e) {
            System.err.println("Failed to send to " + dstHost + ":" + dstPort);
            e.printStackTrace();
        }
    }
}