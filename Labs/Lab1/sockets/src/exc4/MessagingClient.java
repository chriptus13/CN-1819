package exc4;

import contracts.IMessagingService;
import contracts.IMsgBox;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Properties;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MessagingClient {
    private static final String MESSAGING_SERVICE = "MessagingService";
    private static final String svcHost = "10.62.73.69", host = "10.10.39.179";
    private static final int reg_port = 7000, svc_port = 7001;

    private static final Scanner sc = new Scanner(System.in);

    public static void main(String[] args) throws IOException, NotBoundException {
        IMsgBox stub = stubbed(MessagingClient::log);
        IMessagingService service = getService();

        printHeader();

        String username = inputUsername(service, stub);

        /*

        /g msg
        /w [name] msg
        /leave

         */

        Pattern pGlobal = Pattern.compile("/g (.+)"),
                pWhisper = Pattern.compile("/w (\\w+) (.+)"),
                pLeave = Pattern.compile("/leave.*");

        while(true) {
            String msg = sc.nextLine();
            Matcher mG = pGlobal.matcher(msg), mW = pWhisper.matcher(msg), mL = pLeave.matcher(msg);
            if(mG.matches())
                service.sendMulticastMessage(username, mG.group(1));
            else if(mW.matches())
                service.connetUser(mW.group(1))
                        .messageNotification(username, mW.group(2));
            else if(mL.matches()) {
                service.unRegister(username);
                break;
            } else System.out.println("!ERROR! - Invalid command!");
        }
    }

    private static void printHeader() {
        System.out.println("*** ISEL CN CHAT ***");
        System.out.println("Use /g {msg} to send a message to all users");
        System.out.println("Use /w {user} {msg} to whisper a message to user");
        System.out.println("Use /leave to leave chat");
    }

    private static IMessagingService getService() throws RemoteException, NotBoundException {
        Registry reg = LocateRegistry.getRegistry(svcHost, reg_port);
        return (IMessagingService) reg.lookup(MESSAGING_SERVICE);
    }

    private static IMsgBox stubbed(IMsgBox msgBox) throws RemoteException {
        Properties props = System.getProperties();
        props.setProperty("java.rmi.server.hostname", host);
        return (IMsgBox) UnicastRemoteObject.exportObject(msgBox, svc_port);
    }

    private static void log(String username, String msg) {
        System.out.println(String.format("[%s]: %s", username, msg));
    }

    private static String inputUsername(IMessagingService service, IMsgBox stub) throws RemoteException {
        System.out.println("Username?");
        String username = sc.nextLine();
        String resp = service.ping(username);
        System.out.println("[Server]: " + resp);
        if(!service.getRegisteredUsers().contains(username))
            service.register(username, stub);
        return username;
    }
}
