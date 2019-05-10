public class InstanceGroup {
    private final String name;
    private final int numberOfInstances;

    InstanceGroup(String name, int numberOfInstances) {
        this.name = name;
        this.numberOfInstances = numberOfInstances;
    }

    @Override
    public String toString() {
        return "<> " + name + " (Number of instances: " + numberOfInstances + ")";
    }
}
