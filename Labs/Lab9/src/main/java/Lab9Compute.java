import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.ComputeScopes;
import com.google.api.services.compute.model.*;
import com.google.cloud.ServiceOptions;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

class Lab9Compute {
    private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();
    private final Compute service;

    Lab9Compute() throws GeneralSecurityException, IOException {
        HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        GoogleCredential credential = GoogleCredential.getApplicationDefault();
        if (credential.createScopedRequired()) {
            List<String> scopes = new ArrayList<>();
            scopes.add(ComputeScopes.COMPUTE);
            credential = credential.createScoped(scopes);
        }
        service = new Compute.Builder(httpTransport, credential.getJsonFactory(), credential).setApplicationName("Client").build();
    }

    List<String> getInstanceList(String zone) throws IOException {
        Compute.Instances.List instances = service.instances().list(PROJECT_ID, zone);
        InstanceList list = instances.execute();
        List<String> res = new LinkedList<>();
        if (list.getItems() == null) return res;
        list.getItems().forEach(item -> res.add(item.getName()));
        return res;
    }

    void createInstance(String zone, String instanceName, String machineType) throws IOException {
        Instance instance = new Instance();
        instance.setName(instanceName);
        instance.setMachineType(
                "https://www.googleapis.com/compute/v1/projects/"
                        + PROJECT_ID + "/zones/"
                        + zone + "/machineTypes/"
                        + machineType);

        instance.setNetworkInterfaces(Collections.singletonList(createNetworkInterface()));

        instance.setDisks(Collections.singletonList(createDisk(zone, instanceName)));
        Compute.Instances.Insert insert = service.instances().insert(PROJECT_ID, zone, instance);
        Operation op = insert.execute();
        // wait operation ?
    }

    private NetworkInterface createNetworkInterface() {
        NetworkInterface ifc = new NetworkInterface();

        ifc.setNetwork(
                "https://www.googleapis.com/compute/v1/projects/"
                        + PROJECT_ID + "/global/networks/default");
        List<AccessConfig> configs = new ArrayList<>();
        AccessConfig config = new AccessConfig();
        config.setType("ONE_TO_ONE_NAT");
        config.setName("External NAT");
        configs.add(config);
        ifc.setAccessConfigs(configs);
        return ifc;
    }

    private AttachedDisk createDisk(String zone, String instanceName) {
        AttachedDisk disk = new AttachedDisk();
        disk.setBoot(true);
        disk.setAutoDelete(true);
        disk.setType("PERSISTENT");

        AttachedDiskInitializeParams params = new AttachedDiskInitializeParams();

        params.setDiskName(instanceName);
        params.setSourceImage("global/images/" + "lab8-image-stress");
        params.setDiskType(
                "https://www.googleapis.com/compute/v1/projects/"
                        + PROJECT_ID + "/zones/" + zone + "/diskTypes/pd-standard");

        disk.setInitializeParams(params);
        return disk;
    }

    void deleteInstance(String zone, String instanceName) throws IOException {
        Compute.Instances.Delete delete = service.instances().delete(PROJECT_ID, zone, instanceName);
        Operation op = delete.execute();
        // wait operation ?
    }

    List<InstanceGroup> getInstanceGroups(String zone) throws IOException {
        Compute.InstanceGroupManagers.List request = service.instanceGroupManagers().list(PROJECT_ID, zone);
        InstanceGroupManagerList list = request.execute();
        List<InstanceGroup> res = new LinkedList<>();
        if (list.getItems() == null) return res;
        list.getItems().forEach(it -> res.add(new InstanceGroup(it.getName(), it.getTargetSize())));
        return res;
    }

    void resizeInstanceGroup(String zone, String instanceGroupName, int newSize) throws IOException {
        Compute.InstanceGroupManagers.Resize request = service.instanceGroupManagers()
                .resize(PROJECT_ID, zone, instanceGroupName, newSize);
        Operation op = request.execute();
    }
}
