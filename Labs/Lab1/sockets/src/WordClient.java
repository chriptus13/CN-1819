import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class WordClient {
    public static void main(String[] args) throws Exception {
        for(int i = 0; i < 10; i++) {
            run(args[0]);
        }
        return;
    }

    private static void run(String arg) throws IOException {
        Socket client = new Socket("10.62.73.28", Integer.parseInt(arg));

        // Stream to write to
        PrintWriter outSock =
                new PrintWriter(client.getOutputStream(), true);
        // Stream to read from
        BufferedReader inSock =
                new BufferedReader(new InputStreamReader(client.getInputStream()));

        long start = System.currentTimeMillis();
        // write command
        outSock.println("Um exemplo de pangrama em Inglês: The quick brown fox jumps over the lazy dog");
        // read reponse
        System.out.println(inSock.readLine());
        System.out.println("Operation completed in: " + (System.currentTimeMillis() - start) + " ms");

        client.close();
    }
}
