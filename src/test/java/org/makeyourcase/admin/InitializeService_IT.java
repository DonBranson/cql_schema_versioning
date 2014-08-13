package org.makeyourcase.admin;

import java.io.IOException;
import java.io.InputStream;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import org.makeyourcase.persistence.CassandraClusterBuilderMaker;
import org.makeyourcase.persistence.CqlFileRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class InitializeService_IT {

    @Mock
    private CqlFileRunner mockCqlFileRunner;
    @Captor
    private ArgumentCaptor<InputStream> inputStreamCaptor;
    @Mock
    private CassandraClusterBuilderMaker mockCassandraClusterBuilderMaker;
    @Mock
    private Cluster mockCluster;
    @Mock
    private Session mockSession;
    @Mock
    private Cluster.Builder mockClusterBuilder;
    @Mock
    private ResultSet mockResultSet;

    private String testNode = "here";
    private String testKeyspace = "testKeyspace";
    private String testLoadScriptNameCql = "testLoadScriptNameCql";

    @InjectMocks
    private InitializeService subject;

    @Before
    public void setup() throws Exception {
        ReflectionTestUtils.setField(subject, "initializationScriptDirectory", "cql/");
        ReflectionTestUtils.setField(subject, "node", testNode);
        ReflectionTestUtils.setField(subject, "keyspace", testKeyspace);
        ReflectionTestUtils.setField(subject, "loadScriptNameCql", testLoadScriptNameCql);
        when(mockCassandraClusterBuilderMaker.create()).thenReturn(mockClusterBuilder);
        when(mockClusterBuilder.addContactPoint(testNode)).thenReturn(mockClusterBuilder);
        when(mockClusterBuilder.build()).thenReturn(mockCluster);
        when(mockCluster.connect(testKeyspace)).thenReturn(mockSession);
        when(mockSession.execute(eq(testLoadScriptNameCql), anyString())).thenReturn(mockResultSet);
        when(mockResultSet.isExhausted()).thenReturn(true);
    }

    @Test
    public void testThat_Initialize_InvokesCommandsInOrder() throws Exception {
        subject.initialize();
        InOrder inOrder = inOrder(mockCqlFileRunner);
        inOrder.verify(mockCqlFileRunner, atLeast(2)).execute(inputStreamCaptor.capture());
        assertTrue(inputStreamCaptor.getAllValues().get(0).available() > 0);
        assertTrue(inputStreamCaptor.getAllValues().get(1).available() > 0);
    }

    @Test
    public void testThat_Initialize_ConvertsIOException() throws Exception {
        String testMessage = "error";
        doThrow(new IOException(testMessage)).when(mockCqlFileRunner).execute(any(InputStream.class));
        try {
            subject.initialize();
            fail("Should have thrown exception");
        } catch (AdminServiceRuntimeException e) {
            assertEquals(testMessage, e.getCause().getMessage());
        }
    }
}