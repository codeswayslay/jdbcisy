/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.einkar.jdbcisy.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author akinw
 */
public class ConnectionHelper {
    
    private static final Logger logger = LogManager.getLogger(ConnectionHelper.class);
    
    private final String REPORT_HOST = "report.host";
    private final String REPORT_PORT = "report.port";
    private final String REPORT_DB = "report.db";
    private final String REPORT_USERNAME = "report.username";
    private final String REPORT_PASSWORD = "report.password";
    private final String REPORT_DATASOURCE_JNDI = "report.datasource.jndi";
    private final String REPORT_DEPOSITORY = "report.depository";
    
    private final Properties prop;
    private Connection connection;
    private Context ctx;
    private DataSource ds;

    public ConnectionHelper(Properties prop) {
        this.prop = prop;
    }
    
    public Connection getPrepareConnection(ConnectionType type) throws ClassNotFoundException, SQLException, NamingException, Exception {
        switch (type) {
            case MYSQL:
                return prepareMySqlConnection();
            case ORACLE:
                return prepareOracleConnection();
            case SQL_SERVER:
                return prepareSqlServerConnection();
            case DATASOURCE:
                return prepareDatasourceConnection();
            default:
                throw new Exception("Invalid connection type");
        }
    }
    
    /**
     * Gets the database connection. NOTE: the connection could very well be
     * invalid at the time of call.
     * @return the database connection
     */
    public Connection getConnection() {
        return connection;
    }
    
    /**
     * Prepares connection for MySql Server.
     * @return the prepared connection
     * @throws Exception connection is not supported for this library version
     */
    private Connection prepareMySqlConnection() throws Exception {
        throw new Exception("MySql Connection not supported in this library version");
    }
    
    /**
     * Prepares connection for Oracle Server.
     * @return the prepared connection
     * @throws Exception connection is not supported for this library version
     */
    private Connection prepareOracleConnection() throws Exception {
        throw new Exception("Oracle Connection not supported in this library version");
    }
    
    /**
     * Prepares connection for Microsoft SQL Server.
     * @return the prepared connection
     * @throws ClassNotFoundException if the driver class for sql server is not found
     * @throws SQLException if the connection fails, probably due to incorrect login credentials
     */
    private Connection prepareSqlServerConnection() throws ClassNotFoundException, SQLException {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String connectionURL = "jdbc:sqlserver://" + prop.getProperty(REPORT_HOST) + ":" + prop.getProperty(REPORT_PORT)
                + ";databaseName=" + prop.getProperty(REPORT_DB);
        logger.info("Attempting to establish connection to: [{}]", connectionURL);
        connection = DriverManager.getConnection(connectionURL, prop.getProperty(REPORT_USERNAME), prop.getProperty(REPORT_PASSWORD));
        logger.info("Connected to: [{}]", connectionURL);
        return connection;
    }
    
    /**
     * Prepares connection for an established datasource.
     * @return the prepared connection
     * @throws NamingException if the JNDI isn't valid
     * @throws SQLException if the connection fails, probably due to unavailable datasource resources
     */
    private Connection prepareDatasourceConnection() throws NamingException, SQLException {
        ctx = new InitialContext();
        ds = (DataSource) ctx.lookup(prop.getProperty(REPORT_DATASOURCE_JNDI));
        connection = ds.getConnection();
        return connection;
    }
}
