import calcstubs.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Scanner;

public class CalcService extends CalcServiceGrpc.CalcServiceImplBase {
    private static final int svcPort = 7000;

    public static void main(String[] args) throws IOException {
        Server svc = ServerBuilder
                .forPort(svcPort)
                .addService(new CalcService())
                .build()
                .start();
        System.out.println("Server started, listening on " + svcPort);

        System.out.print("Press any key to terminate...");
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        svc.shutdown();
    }

    @Override
    public void add(OperationRequest request, StreamObserver<OperationReply> responseObserver) {
        double op1 = request.getOp1();
        double op2 = request.getOp2();
        OperationReply reply = OperationReply.newBuilder()
                .setRes(op1 + op2)
                .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void findPrimes(NumOfPrimes request, StreamObserver<Prime> responseObserver) {
        int curr = request.getStartNum(),
                max = request.getNumOfPrimes(),
                count = 0;
        for(; count < max; curr++) {
            if(isPrime(curr)) {
                responseObserver
                        .onNext(Prime.newBuilder()
                                .setPrime(curr)
                                .build());
                count++;
            }
        }
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<Num> addAll(StreamObserver<OperationReply> responseObserver) {
        return new StreamObserver<Num>() {
            int sum = 0;

            @Override
            public void onNext(Num num) {
                sum += num.getI();
            }

            @Override
            public void onError(Throwable throwable) {
                responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                responseObserver.onNext(OperationReply.newBuilder().setRes(sum).build());
                responseObserver.onCompleted();
            }
        };
    }

    private boolean isPrime(int n) {
        for(int i = 2; i <= Math.sqrt(n); i++)
            if(n % i == 0) return false;
        return true;
    }
}
