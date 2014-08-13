package org.makeyourcase.persistence;

import com.datastax.driver.core.Cluster;
import org.springframework.stereotype.Component;

@Component
public class CassandraClusterBuilderMaker {

    public Cluster.Builder create(){
        return Cluster.builder();
    }
}
