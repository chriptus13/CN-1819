import java.util.List;

public class CNImage {
    private final String bucketName;
    private final String blobName;
    private final String blobNameWithFaces;
    private final int numberOfFaces;
    private final List<String> labels;

    public CNImage(String bucketName, String blobName, String blobNameWithFaces, int numberOfFaces, List<String> labels) {
        this.bucketName = bucketName;
        this.blobName = blobName;
        this.blobNameWithFaces = blobNameWithFaces;
        this.numberOfFaces = numberOfFaces;
        this.labels = labels;
    }

    public CNImage(String bucketName, String blobName, List<String> labels) {
        this(bucketName, blobName, null, 0, labels);
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getBlobName() {
        return blobName;
    }

    public String getBlobNameWithFaces() {
        return blobNameWithFaces;
    }

    public int getNumberOfFaces() {
        return numberOfFaces;
    }

    public List<String> getLabels() {
        return labels;
    }
}
