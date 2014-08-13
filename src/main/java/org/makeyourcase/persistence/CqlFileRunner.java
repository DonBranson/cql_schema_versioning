package org.makeyourcase.persistence;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.SyntaxError;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class CqlFileRunner {
    
    private static final Logger LOG = Logger.getLogger(CqlFileRunner.class);

    @Value("${cassandra.node}")
    private String node;

    @Value("${cassandra.keyspace}")
    private String keyspace;

    @Autowired
    private CassandraClusterBuilderMaker cassandraClusterBuilderMaker;

    public void execute(InputStream commandStream) throws IOException {
        byte[] commandBuffer = new byte[commandStream.available()];
        IOUtils.readFully(commandStream, commandBuffer);

        Cluster cluster = cassandraClusterBuilderMaker.create().addContactPoint(node).build();
        Session session = cluster.connect(keyspace);

        List<String> commands = Arrays.asList(new String(commandBuffer, "UTF-8").split(";"));
        for(String command : commands){
            String commandLine = command.trim().replaceAll("^-- .*", "");
            if(!commandLine.isEmpty()){
                command = commandLine + ";";
                LOG.info("Execute:\n" + command);
                try {
                    session.execute(command);
                } catch (SyntaxError e) {
                    LOG.error("Command failed with " + e.getMessage());
                    throw e;
                }
            }
        }
        
    }
}