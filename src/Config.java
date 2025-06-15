import java.util.List;

public class Config {
    public static final String MASTER_HOST = "10.30.204.53";
    public static final int MASTER_PORT = 12345;
    public static final NodeInfo MASTER = new NodeInfo(-1, "10.30.204.53", 12345);
    public static final boolean DEBUG = true;

    public static List<NodeInfo> loadWorkers() {
        return List.of(
                new NodeInfo(0, "10.30.204.53", 10001),
                new NodeInfo(1, "10.30.204.53", 10002),
                new NodeInfo(2, "10.30.204.53", 10003)
        );
    }

    public static String getMasterHost() {
        return MASTER_HOST; // or use an IP if deployed
    }

    public static int getNumWorkers() {
        return loadWorkers().size();
    }
}