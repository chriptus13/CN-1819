import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConcurrentWordSearch {
    private static final int SVCPort = 5000;

    static class Session implements Runnable {
        Socket cliSocket;
        int id;
        BufferedReader inStream = null;
        PrintWriter outStream = null;

        public Session(Socket cliSocket, int id) {
            this.cliSocket = cliSocket;
            this.id = id;
            try {
                inStream = new BufferedReader(new InputStreamReader(cliSocket.getInputStream()));
                outStream = new PrintWriter(cliSocket.getOutputStream(), true);
            } catch(IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            try {
                String line = inStream.readLine();
                String[] tmp = line.split(" ");
                String bigWord = "";
                for(String s : tmp) {
                    if(s.length() > bigWord.length()) {
                        bigWord = s;
                    }
                }
                // simula tempo de processamento
                Thread.sleep(10 * 1000);
                // ...
                outStream.println("biggest word: " + bigWord + " size = " + bigWord.length());
                cliSocket.close();
                System.out.println("Session " + id + " terminates");
            } catch(Exception ex) {
                ex.printStackTrace();
                System.out.println("Server session " + id + " crashed!");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(5);
        ServerSocket svcSocket = new ServerSocket(SVCPort);
        int sessionId = 0;
        for(; ; ) {
            System.out.println("Accepting new connections... ");
            Socket client = svcSocket.accept();
            System.out.println("New connection with... " + client);
            Runnable worker = new Session(client, sessionId++);
            executor.execute(worker);
        }
    }
}
