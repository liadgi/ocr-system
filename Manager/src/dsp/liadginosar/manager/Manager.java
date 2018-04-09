package dsp.liadginosar.manager;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Manager {

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Manager started, listening on SQS:");
        TimeUnit.SECONDS.sleep(5);
        System.out.println("Manager stopped.");
    }
}
