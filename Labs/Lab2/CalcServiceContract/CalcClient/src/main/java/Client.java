import calcstubs.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.Iterator;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Client {
    private static final Scanner sc = new Scanner(System.in);
    private static final Pattern ptAdd = Pattern.compile("/add (\\d+)\\+(\\d+)"),
            ptPrime = Pattern.compile("/primes (\\d+)->(\\d+)"),
            ptAddBlocking = Pattern.compile("/addBlocking (\\d+)\\+(\\d+)"),
            ptPrimeBlocking = Pattern.compile("/primesBlocking (\\d+)->(\\d+)"),
            ptAddAll = Pattern.compile("/add( \\d+)+");
    private static final String svc_ip = "10.62.73.28";
    private static final int svc_port = 7000;
    private static ManagedChannel ch = ManagedChannelBuilder
            .forAddress(svc_ip, svc_port)
            .usePlaintext()
            .build();
    private static CalcServiceGrpc.CalcServiceBlockingStub bStub = CalcServiceGrpc.newBlockingStub(ch);
    private static CalcServiceGrpc.CalcServiceStub stub = CalcServiceGrpc.newStub(ch);

    public static void main(String[] args) {
        do {
            String line = sc.nextLine();
            if(line.startsWith("/exit")) break;
            Matcher mAdd = ptAdd.matcher(line), mPrime = ptPrime.matcher(line),
                    mAddBlocking = ptAddBlocking.matcher(line), mPrimeBlocking = ptPrimeBlocking.matcher(line),
                    mAddAll = ptAddAll.matcher(line);
            if(mAdd.matches())
                add(Integer.parseInt(mAdd.group(1)), Integer.parseInt(mAdd.group(2)));
            else if(mPrime.matches())
                primes(Integer.parseInt(mPrime.group(1)), Integer.parseInt(mPrime.group(2)));
            else if(mAddBlocking.matches())
                addBlocking(Integer.parseInt(mAddBlocking.group(1)), Integer.parseInt(mAddBlocking.group(2)));
            else if(mPrimeBlocking.matches())
                primesBlocking(Integer.parseInt(mPrimeBlocking.group(1)), Integer.parseInt(mPrimeBlocking.group(2)));
            else if(mAddAll.matches()) {
                StreamObserver<Num> numStreamObserver = stub.addAll(new StreamObserver<OperationReply>() {
                    @Override
                    public void onNext(OperationReply operationReply) {
                        System.out.println("Result = " + (int) operationReply.getRes());
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        System.out.println("ERROR " + throwable.getMessage());
                    }

                    @Override
                    public void onCompleted() { }
                });
                String[] values = line.split(" ");
                for(int i = 1; i < values.length; ++i) numStreamObserver.onNext(Num.newBuilder().setI(Integer.parseInt(values[i])).build());
                numStreamObserver.onCompleted();
            } else
                System.out.println("INVALID COMMAND U DUMBASS!");
        } while(true);

    }

    private static void add(int a, int b) {
        stub.add(
                OperationRequest.newBuilder()
                        .setOp1(a)
                        .setOp2(b)
                        .build(),
                new StreamObserver<OperationReply>() {
                    @Override
                    public void onNext(OperationReply operationReply) {
                        System.out.println(a + " + " + b + " = " + (int) operationReply.getRes());
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        System.out.println("Error: " + throwable.getMessage());
                    }

                    @Override
                    public void onCompleted() { }
                }
        );
    }

    private static void addBlocking(int a, int b) {
        double res = bStub.add(
                OperationRequest.newBuilder()
                        .setOp1(a)
                        .setOp2(b)
                        .build())
                .getRes();

        System.out.println(a + " + " + b + " = " + (int) res);

    }

    private static void primes(int start, int n) {
        stub.findPrimes(
                NumOfPrimes.newBuilder()
                        .setStartNum(start)
                        .setNumOfPrimes(n)
                        .build(),
                new StreamObserver<Prime>() {
                    int t = 1;

                    @Override
                    public void onNext(Prime prime) {
                        System.out.println(t++ + "#Prime = " + prime.getPrime());
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        System.out.println("Error: " + throwable.getMessage());
                    }

                    @Override
                    public void onCompleted() {
                        System.out.println("DONE");
                    }
                }
        );
    }

    private static void primesBlocking(int start, int n) {
        Iterator<Prime> primes = bStub.findPrimes(NumOfPrimes.newBuilder().setStartNum(start).setNumOfPrimes(n).build());
        int i = 1;
        while(primes.hasNext()) System.out.println(i++ + "#Prime = " + primes.next().getPrime());
    }
}
