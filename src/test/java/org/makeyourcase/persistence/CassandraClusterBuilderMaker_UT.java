package org.makeyourcase.persistence;

import com.datastax.driver.core.Cluster;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertSame;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Cluster.class})
public class CassandraClusterBuilderMaker_UT {

    @Mock
    private Cluster.Builder mockBuilder;

    @InjectMocks
    private CassandraClusterBuilderMaker subject;

    @Test
    public void testThat_Create_ProvidesABuilder() throws Exception {
        whenNew(Cluster.Builder.class).withNoArguments().thenReturn(mockBuilder);
        Cluster.Builder actualBuilder = subject.create();
        assertSame(mockBuilder, actualBuilder);
    }

}