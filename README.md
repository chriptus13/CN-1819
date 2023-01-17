# CN-1819

## How to run

1. Confirm that topics ```Topic_T1``` and ```CN_Photos_Metrics``` exist such as subscriptions ```Subscription_A``` and ```CN_Photos_Metrics_Subscription```.

    **Topic_T1**: topic where will be publish images to process.

    **Subscription_A**: subscription which all workers will be subscribers.

    **CN_Photos_Metrics**: topic where will be publish metrics of the workers.

    **CN_Photos_Metrics_Subscription**: subscription which the component of management will be subscribe to receive all metrics off the workers.

2. Confirm that the instance group ```cn-photos-group``` exist at zone ```us-east1-b``` or create one with the same name at the same zone using ```cn-photos-worker``` instance template.

3. Confirm that VM instance responsable for monitoring is running or create one with ```cn-photos-monitor``` instance template.

4. Confirm that bucket ```cnphotos-g06``` exist.

5. Create JSON key from the service account ```cn-photos@g06-li-leirt61d.iam.gserviceaccount.com``` and set environment variable *GOOGLE_APPLICATION_CREDENTIALS* with path to the JSON jey created.

6. Build contract jar.

    ```bash
    cd Trabalho-Final/CNMonitorContract
    mvn package
    ```

7. Build client application jar.

    ```bash
    cd Trabalho-Final/CNPhotosClient
    mvn package
    ```

8. Run client application (console application).

    * CJP - contract jar path.
    * CAJP - client application jar path.
    * EIP - External IP of VM instance responsable for monitoring.

    ```bash
    cd Trabalho-Final/CNPhotosClient
    java -cp {CJP}:{CAJP} Client {EIP}
    ```

9. For more information about the commands to use enter ```/help```.
