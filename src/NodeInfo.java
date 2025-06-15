import java.io.Serializable;

public class NodeInfo implements Serializable {
    public final int id;
    public final String hostname;
    public final int port;

    public NodeInfo(int id, String hostname, int port) {
        this.id = id;
        this.hostname = hostname;
        this.port = port;
    }

    @Override
    public String toString() {
        return "NodeInfo{" + "id=" + id + ", hostname='" + hostname + '\'' + ", port=" + port + '}';
    }
}