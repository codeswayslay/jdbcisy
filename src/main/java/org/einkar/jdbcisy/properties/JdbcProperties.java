/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.einkar.jdbcisy.properties;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author akinwale.agbaje
 */
public class JdbcProperties extends Properties {
    private static final Logger logger = LogManager.getLogger(JdbcProperties.class);
    
    private InputStream instream;
    private static JdbcProperties INSTANCE;
    private final String pathToFile;
    
    private JdbcProperties(String pathToFile) throws Exception {
        this.pathToFile = pathToFile;
        load();
    }

    private JdbcProperties() throws Exception {
        this.pathToFile = "";
        load();
    }
    
    /**
     * Creates single instance of JDBC properties while allowing for the loading of a customised JDBC config properties file
     * @param pathToFile the path and file name of the config file
     * @throws Exception 
     */
    public static JdbcProperties getInstance(String pathToFile) throws Exception {
        if (INSTANCE == null) {
            INSTANCE = new JdbcProperties(pathToFile);
        }
        return INSTANCE;
    }
    
    /**
     * Creates single instance of JDBC properties 
     * @return the instance
     * @throws Exception 
     */
    public static JdbcProperties getInstance() throws Exception {
        if (INSTANCE == null) {
            INSTANCE = new JdbcProperties();
        }
        return INSTANCE;
    }
    
    /**
     * Attempts to load from customised file. If file not found, default to in-memory config file. 
     * If that isn't found, throw exception
     * @throws Exception if file not found or I/O error occurs
     */
    private void load() throws Exception {
        boolean loadedFromFile = false;
        
        if (pathToFile != null && !pathToFile.trim().isEmpty()) {
            File propFile = new File(pathToFile);
            if (propFile.exists()) {
                try {
                    FileInputStream fileinstream = new FileInputStream(propFile);
                    load(fileinstream);
                    fileinstream.close();
                    loadedFromFile = true;
                } catch (FileNotFoundException ex) {
                    logger.info("Error locating file to load for jdbc properties. Defaulting to in-memory file");
                    logger.error("Error locating file to load for jdbc properties", ex);
                } catch (IOException ex) {
                    logger.info("Error reading file to load for jdbc properties. Defaulting to in-memory file");
                    logger.error("Error reading file to load for jdbc properties", ex);
                }
            } else {
                logger.info("Error locating file to load for jdbc properties. Defaulting to in-memory file");
            }
        }
        
        if (!loadedFromFile) {
            try {
                String inmemoryfile = "jdbcconnection.properties";
                instream = JdbcProperties.class.getClassLoader().getResourceAsStream(inmemoryfile);
                load(instream);
                instream.close();
            } catch (IOException ex) {
                logger.error("Error reading in-memory file to load for jdbc properties", ex);
                throw new Exception("Error reading in-memory file to load for jdbc properties: " + ex);
            }
        }
    }
}
