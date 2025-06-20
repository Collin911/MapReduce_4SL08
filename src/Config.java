import java.util.List;

public class Config {
    public static final NodeInfo MASTER = new NodeInfo(-1, "137.194.249.148", 12345);
    public enum outType {
        DEBUG,
        INFO,
        WARN,
        ERR
    }
    public static final outType OUT = outType.INFO;

    public static List<NodeInfo> loadWorkers() {
        return List.of(
                new NodeInfo(0, "tp-1d22-01", 10001),
                new NodeInfo(1, "tp-1d22-02", 10002),
                new NodeInfo(2, "tp-1d22-03", 10003)
        );
    }

    public static int getNumWorkers() {
        return loadWorkers().size();
    }

    public static void consoleOutput(outType type, String msg){
        switch (type){
            case DEBUG:
                if (OUT == outType.DEBUG)
                    System.out.println(msg);
                break;
            case INFO:
                if (OUT == outType.DEBUG || OUT == outType.INFO)
                    System.out.println(msg);
                break;
            case WARN:
                if (OUT == outType.DEBUG || OUT == outType.INFO || OUT == outType.WARN)
                    System.err.println(msg);
                break;
            case ERR:
                System.err.println(msg);
                break;

        }
    }
}