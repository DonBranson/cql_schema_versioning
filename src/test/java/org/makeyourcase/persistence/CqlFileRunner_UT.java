package org.makeyourcase.persistence;

import java.io.ByteArrayInputStream;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CqlFileRunner_UT {
    
    private String testNode = "here";
    private String testKeyspace = "testKeyspace";

    @Mock
    private CassandraClusterBuilderMaker mockCassandraClusterBuilderMaker;
    @Mock
    private Cluster mockCluster;
    @Mock
    private Session mockSession;
    @Mock
    private Cluster.Builder mockClusterBuilder;

    @InjectMocks
    private CqlFileRunner subject;

    @Test
    public void testThat_Execute_RunsCommandsFromFile() throws Exception {
        ReflectionTestUtils.setField(subject, "node", testNode);
        ReflectionTestUtils.setField(subject, "keyspace", testKeyspace);
        String commands = "command1;\ncommand2;";
        when(mockCassandraClusterBuilderMaker.create()).thenReturn(mockClusterBuilder);
        when(mockClusterBuilder.addContactPoint(testNode)).thenReturn(mockClusterBuilder);
        when(mockClusterBuilder.build()).thenReturn(mockCluster);
        when(mockCluster.connect(testKeyspace)).thenReturn(mockSession);

        subject.execute(new ByteArrayInputStream(commands.getBytes()));

        InOrder inOrder = inOrder(mockSession);
        inOrder.verify(mockSession).execute("command1;");
        inOrder.verify(mockSession).execute("command2;");
    }
}