package lib;

import java.io.IOException;
import java.util.Properties;

public class GetProperties {

    public GetProperties() {
    }

    public static Properties getConfluentCloudProperties() {
        Properties props = new Properties();
        try {
            props.load(
                    ClassLoader.getSystemResourceAsStream("confluent_cloud.properties")
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }

    /*public Properties getInstance(int cloudName) {
        try {
            if (props.isEmpty()) {
                getProps(cloudName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }*/

}
