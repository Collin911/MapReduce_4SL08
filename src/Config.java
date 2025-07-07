import java.util.List;

public class Config {
    //public static final NodeInfo MASTER = new NodeInfo(-1, "137.194.125.65", 12345);
    public static final NodeInfo MASTER = new NodeInfo(-1, "localhost", 12345);
    public enum outType {
        DEEP,
        DEBUG,
        INFO,
        WARN,
        ERR
    }
    public static final outType OUT = outType.DEBUG;
    public static final String finalResultOutput = "KEEP_LOCAL";
    // Tell the slave to send result explicitly or keep result locally
    // use "EXPLICIT" or "KEEP_LOCAL" as control string

    // Specify the workers' info here and ONLY HERE
    // Comment out those not needed
    public static List<NodeInfo> loadWorkers() {
        return List.of(
                new NodeInfo(0, "localhost", 10001)
                //,new NodeInfo(1, "tp-1d22-02", 10002)
                //,new NodeInfo(2, "tp-1d22-03", 10003)
                //,new NodeInfo(3, "tp-1d22-04", 10004)
                //,new NodeInfo(4, "tp-1d22-05", 10005)
                //,new NodeInfo(5, "tp-1d22-06", 10006)
                //,new NodeInfo(6, "tp-1d22-08", 10007)
                //,new NodeInfo(7, "tp-1d22-09", 10008)
                //,new NodeInfo(8, "tp-1d22-10", 10009)
                //,new NodeInfo(9, "tp-1d22-11", 10010)

        );
    }

    public static void consoleOutput(outType type, String msg){
        switch (type){
            case DEEP:
                if (OUT == outType.DEEP)
                    System.out.println(msg);
                break;
            case DEBUG:
                if (OUT == outType.DEBUG || OUT == outType.DEEP)
                    System.out.println(msg);
                break;
            case INFO:
                if (OUT == outType.DEBUG || OUT == outType.INFO || OUT == outType.DEEP)
                    System.out.println(msg);
                break;
            case WARN:
                if (OUT == outType.DEBUG || OUT == outType.INFO || OUT == outType.WARN || OUT == outType.DEEP)
                    System.err.println(msg);
                break;
            case ERR:
                System.err.println(msg);
                break;

        }
    }
}