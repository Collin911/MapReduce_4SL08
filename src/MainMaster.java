import java.io.IOException;

public class MainMaster {
    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("Usage: java MainMaster <file1> <file2> ...");
            return;
        }

        MasterNode master = new MasterNode(args);
        master.start();
    }
}