import java.io.IOException;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Client {

    private static final Pattern ptInstances = Pattern.compile("/showI (.+)"),
            ptCreateInstance = Pattern.compile("/newI (.+);(.+);(.+)"),
            ptDeleteInstance = Pattern.compile("/deleteI (.+);(.+)"),
            ptInstanceGroups = Pattern.compile("/showIG (.+)"),
            ptResizeInstanceGroups = Pattern.compile("/resizeIG (.+);(.+);(.+)");
    private static Lab9Compute computeService;

    public static void main(String[] args) {
        try {
            computeService = new Lab9Compute();

            Scanner in = new Scanner(System.in);

            showHelp();

            do {
                String line = in.nextLine();

                if (line.startsWith("/exit")) break;

                Matcher mInstances = ptInstances.matcher(line),
                        mCreateInstance = ptCreateInstance.matcher(line),
                        mDeleteInstance = ptDeleteInstance.matcher(line),
                        mInstanceGroups = ptInstanceGroups.matcher(line),
                        mResizeInstanceGroups = ptResizeInstanceGroups.matcher(line);

                try {

                    if (mInstances.matches())
                        showInstanceList(mInstances.group(1));
                    else if (mCreateInstance.matches())
                        createNewInstance(mCreateInstance.group(1), mCreateInstance.group(2), mCreateInstance.group(3));
                    else if (mDeleteInstance.matches())
                        deleteInstance(mDeleteInstance.group(1), mDeleteInstance.group(2));
                    else if (mInstanceGroups.matches())
                        showInstanceGroups(mInstanceGroups.group(1));
                    else if (mResizeInstanceGroups.matches())
                        resizeInstanceGroup(mResizeInstanceGroups.group(1), mResizeInstanceGroups.group(2),
                                Integer.parseInt(mResizeInstanceGroups.group(3)));
                    else if (line.startsWith("/help"))
                        showHelp();
                    else
                        System.out.println("Invalid Command!");
                } catch (Exception e) {
                    System.out.println("Error: " + e.getMessage());
                }

            } while (true);

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private static void showInstanceList(String zone) throws IOException {
        List<String> list = computeService.getInstanceList(zone);
        System.out.println("*** Instances in zone " + zone + " ***");
        list.forEach(System.out::println);
        System.out.println("*** ** ***");
    }

    private static void createNewInstance(String zone, String instanceName, String machineType) throws IOException {
        computeService.createInstance(zone, instanceName, machineType);
        System.out.println("Instance created");
    }

    private static void deleteInstance(String zone, String instanceName) throws IOException {
        computeService.deleteInstance(zone, instanceName);
        System.out.println("Instance deleted");
    }

    private static void showInstanceGroups(String zone) throws IOException {
        List<InstanceGroup> list = computeService.getInstanceGroups(zone);
        System.out.println("*** Instance Groups in zone " + zone + " ***");
        list.forEach(System.out::println);
        System.out.println("*** ** ***");
    }

    private static void resizeInstanceGroup(String zone, String instanceGroupName, int newSize) throws IOException {
        computeService.resizeInstanceGroup(zone, instanceGroupName, newSize);
        System.out.println("Instance Group resized");
    }

    private static void showHelp() {
        System.out.println("*** Help ***");
        System.out.println("/help - show commands");
        System.out.println("/showI <zone> - show all instances in <zone>");
        System.out.println("/newI <zone>;<name>;<machineType> - create new instance");
        System.out.println("/deleteI <zone>;<name> - delete instance in <zone> with <name>");
        System.out.println("/showIG <zone> - show all instance groups in <zone>");
        System.out.println("/resizeIG <zone>;<name>;<size> - resize instance group in <zone> with <name>");
        System.out.println("/exit - leave");
        System.out.println("*** **** ***");
    }
}
