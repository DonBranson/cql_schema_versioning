package org.makeyourcase.admin;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import org.makeyourcase.persistence.CassandraClusterBuilderMaker;
import org.makeyourcase.persistence.CqlFileRunner;

@Component
public class InitializeService {

    private static Logger LOG = Logger.getLogger(InitializeService.class);

    @Value("${initialization.script.root}")
    private String initializationScriptDirectory;

    @Autowired
    private CqlFileRunner cqlFileRunner;

    @Value("${cassandra.node}")
    private String node;

    @Value("${cql.version.table.create}")
    private String createVersionTableCql;

    @Value("${cql.version.load}")
    private String loadScriptNameCql;

    @Value("${cql.version.record}")
    private String recordScriptNameCql;

    @Value("${cassandra.keyspace}")
    private String keyspace;

    @Autowired
    private CassandraClusterBuilderMaker cassandraClusterBuilderMaker;

    public void initialize() throws URISyntaxException {
        try {
            Cluster cluster = cassandraClusterBuilderMaker.create().addContactPoint(node).build();
            Session session = cluster.connect(keyspace);
            session.execute(createVersionTableCql);

            String[] cqlScripts = getResourceListing(getClass(), initializationScriptDirectory);
            for(String cqlScript : new TreeSet<>(Arrays.asList(cqlScripts))){
                LOG.info("------------------------------------------------------------");
                if(scriptHasBeenRun(session, cqlScript)) {
                    LOG.info("Skipping script " + cqlScript + ", it's already been run.");
                } else {
                    LOG.info("Running script " + cqlScript);
                    cqlFileRunner.execute(Thread.currentThread().getContextClassLoader().getResourceAsStream(initializationScriptDirectory + cqlScript));
                    recordScript(session, cqlScript);
                }
            }
        } catch (IOException e) {
            throw new AdminServiceRuntimeException(e);
        }
    }

    private void recordScript(Session session, String cqlScript) {
        session.execute(recordScriptNameCql, cqlScript);
    }

    private boolean scriptHasBeenRun(Session session, String cqlScript) {
        return !session.execute(loadScriptNameCql, cqlScript).isExhausted();
    }

    /**
     * List directory contents for a resource folder. Not recursive.
     * This is basically a brute-force implementation.
     * Works for regular files and also JARs.
     *
     * @author Greg Briggs
     * @param clazz Any java class that lives in the same place as the resources you want.
     * @param path Should end with "/", but not start with one.
     * @return Just the name of each member item, not the full paths.
     * @throws URISyntaxException
     * @throws IOException
     */
    private String[] getResourceListing(Class clazz, String path) throws URISyntaxException, IOException {
        URL dirURL = clazz.getClassLoader().getResource(path);
        if (dirURL != null && "file".equals(dirURL.getProtocol())) {
            return processFileSystemPath(dirURL);
        }

        if (dirURL == null) {
            dirURL = buildUrlPointingIntoJar(clazz);
        }

        if ("jar".equals(dirURL.getProtocol())) {
            Set<String> result = processJarPath(path, dirURL);
            return result.toArray(new String[result.size()]);
        }

        throw new UnsupportedOperationException("Cannot list files for URL "+dirURL);
    }

    private URL buildUrlPointingIntoJar(Class clazz) {
        return clazz.getClassLoader().getResource(clazz.getName().replace(".", "/")+".class");
    }

    private String[] processFileSystemPath(URL dirURL) throws URISyntaxException {
        return new File(dirURL.toURI()).list();
    }

    private Set<String> processJarPath(String path, URL dirURL) throws IOException {
        JarFile jar = new JarFile(URLDecoder.decode(stripOutTheOnlyJarFile(dirURL), "UTF-8"));
        Enumeration<JarEntry> entries = jar.entries();
        // Use set avoid duplicates in case it is a subdirectory
        Set<String> result = new HashSet<>();
        while(entries.hasMoreElements()) {
            String name = entries.nextElement().getName();
            if (excludeFilesNotInPath(path, name)) {
                result.add(convertJarEntry(path, name));
            }
        }
        jar.close();
        return result;
    }

    private boolean excludeFilesNotInPath(String path, String name) {
        return name.startsWith(path);
    }

    private String stripOutTheOnlyJarFile(URL dirURL) {
        return dirURL.getPath().substring(5, dirURL.getPath().indexOf("!"));
    }

    private String convertJarEntry(String path, String name) {
        String entry = name.substring(path.length());
        int firstSlash = entry.indexOf('/');
        if (entryIsASubdirectory(firstSlash)) {
            entry = extractDirectoryName(entry, firstSlash);
        }
        return entry;
    }

    private String extractDirectoryName(String entry, int firstSlash) {
        return entry.substring(0, firstSlash);
    }

    private boolean entryIsASubdirectory(int checkSubdir) {
        return checkSubdir >= 0;
    }
}