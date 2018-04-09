package dsp.liadginosar.localapp;

import java.io.IOException;

public class LocalApplication {
    public static void main(String[] args) throws IOException, Exception {
        if (args.length == 2) {
            String filename = args[0];
            int numOfWorkers = Integer.parseInt(args[1]);

            System.out.println("Filename: " + filename);
            System.out.println("workers #:" + numOfWorkers);

        } else {
            throw new Exception("Incorrect number of arguments.");
        }

    }
}
