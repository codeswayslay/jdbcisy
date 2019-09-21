/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.einkar.jdbcisy;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.einkar.jdbcisy.properties.JdbcProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author akinwale.agbaje
 * @deprecated will no longer be updated. Utilise {@link DatabaseDatasourceEngine}
 */
public class DatabaseEngine {

    private static final Logger logger = LogManager.getLogger(DatabaseEngine.class);

    private final String REPORT_HOST = "report.host";
    private final String REPORT_PORT = "report.port";
    private final String REPORT_DB = "report.db";
    private final String REPORT_USERNAME = "report.username";
    private final String REPORT_PASSWORD = "report.password";
    private final String REPORT_DEPOSITORY = "report.depository";

    private Properties prop;
    private Connection connection;

    /**
     * Starts up the database engine using in-memory jdbc configuration.
     * NOTE: this engine only works with MICROSOFT SQL SERVER
     *
     * @throws Exception if SQL error occurs or microsoft driver class not found
     */
    public DatabaseEngine() throws Exception {
        try {
            this.prop = JdbcProperties.getInstance();
            openConnection();
        } catch (ClassNotFoundException | SQLException ex) {
            logger.info("An exception has been thrown while trying connect to the database");
            logger.error("An exception has been thrown while trying connect to the database", ex);
            throw new Exception("An exception has been thrown while trying connect to the database. Please contact Administrator. Exception: " + ex);
        }
    }

    /**
     * Starts up the database engine using customised jdbc configuration file.
     * NOTE: this engine only works with MICROSOFT SQL SERVER
     *
     * @param propFile path and name of the jdbc configuration file to load
     * @throws Exception if SQL error occurs or microsoft driver class not found
     */
    public DatabaseEngine(String propFile) throws Exception {
        try {
            this.prop = JdbcProperties.getInstance(propFile);
            openConnection();
        } catch (ClassNotFoundException | SQLException ex) {
            logger.info("An exception has been thrown while trying connect to the database");
            logger.error("An exception has been thrown while trying connect to the database", ex);
            throw new Exception("An exception has been thrown while trying connect to the database. Please contact Administrator. Exception: " + ex);
        }
    }

    /**
     * Starts up the database engine using customised jdbc configuration
     * properties object. NOTE: this engine only works with MICROSOFT SQL SERVER
     *
     * @param prop the properties object to use
     * @throws Exception if SQL error occurs or microsoft driver class not found
     */
    public DatabaseEngine(Properties prop) throws Exception {
        try {
            this.prop = prop;
            openConnection();
        } catch (ClassNotFoundException | SQLException ex) {
            logger.info("An exception has been thrown while trying connect to the database");
            logger.error("An exception has been thrown while trying connect to the database", ex);
            throw new Exception("An exception has been thrown while trying connect to the database. Please contact Administrator. Exception: " + ex);
        }
    }

    /**
     * Opens a connection to the database. Configuration parameters are loaded
     * from the jdbc configuration properties file.
     *
     * @throws ClassNotFoundException if driver class not found (in this case,
     * microsoft)
     * @throws SQLException
     */
    private void openConnection() throws ClassNotFoundException, SQLException {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String connectionURL = "jdbc:sqlserver://" + prop.getProperty(REPORT_HOST) + ":" + prop.getProperty(REPORT_PORT)
                + ";databaseName=" + prop.getProperty(REPORT_DB);
        logger.info("Attempting to establish connection to: [{}]", connectionURL);
        connection = DriverManager.getConnection(connectionURL, prop.getProperty(REPORT_USERNAME), prop.getProperty(REPORT_PASSWORD));
        logger.info("Connected to: [{}]", connectionURL);
    }

    /**
     * Runs the provided sql script.
     *
     * @param sqlStatement the sql statement script
     * @param parameters the parameters to feed into the scripts
     * @return the result list, containing two items: (1) the headers, (2) the
     * data
     * @throws SQLException
     * @throws Exception
     */
    public List runStatement(String sqlStatement, Object... parameters) throws SQLException, Exception {
        if (parameters.length != countParameterBlocksInStatement(sqlStatement)) {
            logger.info("The parameter blocks in the statement does not match the number of parameters passed");
            throw new Exception("The parameter blocks in the statement does not match the number of parameters passed");
        }

        PreparedStatement statement = connection.prepareStatement(sqlStatement);
        statement = insertParameters(statement, parameters);
        ResultSet rs = statement.executeQuery();

        List resultList = extractDataFromResultset(rs);
        statement.close();

        return resultList;
    }

    /**
     * A standard sql escape syntax to runs stored procedure without parameter
     *[this is peculiar for script that includes DML, DDL or/and DCL]
     * @param sqlStatement the sql statement script
     * @param parameters the parameters to feed into the scripts
     * @return the result list, containing two items: (1) the headers, (2) the
     * data
     * @throws SQLException
     * @throws Exception
     */
    public List runNonParameterizedProcedureStatement(String sqlStatement, Object... parameters) throws SQLException, Exception {
        if (parameters.length != countParameterBlocksInStatement(sqlStatement)) {
            logger.info("The parameter blocks in the statement does not match the number of parameters passed");
            throw new Exception("The parameter blocks in the statement does not match the number of parameters passed");
        }

        logger.info("the sql escape syntax eventually sent to server is", sqlStatement);
        CallableStatement statement = connection.prepareCall(sqlStatement);
        statement = insertProcedureParameters(statement, parameters);
        ResultSet rs = null;
        if (statement.execute()) {
            rs = statement.getResultSet();
        } else {
            logger.info("No record is found is the resultset object...");
        }

        List resultList = extractDataFromResultset(rs);
        return resultList;
    }
    
    /**
     * A standard sql escape syntax to runs stored procedure without parameter
     *[this is peculiar for script that includes DML, DDL or/and DCL]
     * @param sqlStatement the sql statement script
     * @param parameters the parameters to feed into the scripts
     * @return the result list, containing two items: (1) the headers, (2) the
     * data
     * @throws SQLException
     * @throws Exception
     */
    public ResultSet runNonParameterizedProcedureStatement_ResultSet(String sqlStatement, Object... parameters) throws SQLException, Exception {
        if (parameters.length != countParameterBlocksInStatement(sqlStatement)) {
            logger.info("The parameter blocks in the statement does not match the number of parameters passed");
            throw new Exception("The parameter blocks in the statement does not match the number of parameters passed");
        }

        logger.info("the sql escape syntax eventually sent to server is", sqlStatement);
        CallableStatement statement = connection.prepareCall(sqlStatement);
        statement = insertProcedureParameters(statement, parameters);
        ResultSet rs = null;
        if (statement.execute()) {
            rs = statement.getResultSet();
        } else {
            logger.info("No record is found is the resultset object...");
        }
        return rs;
    }

    /**
     * Dynamically inserts query parameter into prepared statement script.
     *
     * @param statement the prepared statement
     * @param parameters the parameters to input into the prepared statement
     * @return the prepared statement with parameters in them
     * @throws SQLException
     */
    private PreparedStatement insertParameters(PreparedStatement statement, Object... parameters) throws SQLException {
        for (int i = 0; i < parameters.length; i++) {
            statement.setObject(i + 1, parameters[i]);//at index 0 for paramters, the first parameter is set to 1, hence i+1
        }
        return statement;
    }

    /**
     * Dynamically inserts query parameter into prepared statement script.
     *
     * @param statement the prepared statement
     * @param parameters the parameters to input into the prepared statement
     * @return the prepared statement with parameters in them
     * @throws SQLException
     */
    private CallableStatement insertProcedureParameters(CallableStatement statement, Object... parameters) throws SQLException {
        for (int i = 0; i < parameters.length; i++) {
            statement.setObject(i + 1, parameters[i]);//at index 0 for paramters, the first parameter is set to 1, hence i+1
        }
        return statement;
    }

    /**
     * Counts the number of parameter entries required by the sql script.
     * Parameter blocks are denoted with a question mark (?)
     *
     * @param sqlStatement the sql script
     * @return the number of parameter entries
     */
    private int countParameterBlocksInStatement(String sqlStatement) {
        char where = '?';
        int count = 0;
        for (int i = 0; i < sqlStatement.length(); i++) {
            if (sqlStatement.charAt(i) == where) {
                count += 1;
            }
        }
        return count;
    }

    /**
     * Extracts the headers and data content from the prepared statement's
     * result set.
     *
     * @param resultset the result set
     * @return the list containing the headers and data content
     * @throws Exception
     */
    private List extractDataFromResultset(ResultSet resultset) throws Exception {
        List<String> columnheaders = new ArrayList<>();
        List<Object[]> datacontent = new ArrayList<>();
        List records = new ArrayList();

        if (resultset != null) {
            int noOfColumns = resultset.getMetaData().getColumnCount();

            //get columns
            columnheaders.add("S/N");
            for (int i = 0; i < noOfColumns; i++) {
                columnheaders.add(resultset.getMetaData().getColumnLabel(i + 1));//value in result set starts from 1
            }

            //get data contents
            while (resultset.next()) {
                if (noOfColumns <= 0) {
                    logger.info("Result set is empty");
                }

                Object[] resultsetArray = new Object[noOfColumns];
                for (int i = 0; i < noOfColumns; i++) {
                    //gather each column content into a row
                    //value in result set starts from 1, hence the i + 1
                    resultsetArray[i] = resultset.getObject(i + 1);
                }
                //insert row into data content list
                datacontent.add(resultsetArray);
            }
            records.add(columnheaders);
            records.add(datacontent);

            resultset.close();
        }

        return records;
    }

    /**
     * Commits the transaction. Must be called after
     * {@link #runStatement(java.lang.String, java.lang.Object...)}
     *
     * @throws SQLException
     */
    public void commitTransaction() throws SQLException {
        connection.commit();
    }

    /**
     * Closes the connection to the database. Must be called after
     * {@link #commitTransaction()}
     *
     * @throws SQLException
     */
    public void closeConnection() throws SQLException {
        connection.close();
    }

    /**
     * Gets the database connection. NOTE: the connection could very well be
     * invalid at the time of call.
     *
     * @return
     */
    public Connection getDatabaseConnection() {
        return connection;
    }

    /**
     * Returns the depository set in the jdbc configuration file. The depository
     * path should be the location where reports are stored, although the user
     * isn't required to use it
     *
     * @return
     */
    public String returnDepository() {
        return prop.getProperty(REPORT_DEPOSITORY);
    }

    /**
     * Runs the provided sql script.
     *
     * @param sqlStatement the sql statement script
     * @param parameters the parameters to feed into the scripts
     * @return the result list, containing two items: (1) the headers, (2) the
     * data
     * @throws SQLException
     * @throws Exception
     */
    public ResultSet returnResultSet(String sqlStatement, Object... parameters) throws SQLException, Exception {
        if (parameters.length != countParameterBlocksInStatement(sqlStatement)) {
            logger.info("The parameter blocks in the statement does not match the number of parameters passed");
            throw new Exception("The parameter blocks in the statement does not match the number of parameters passed");
        }

        PreparedStatement statement = connection.prepareStatement(sqlStatement);
        statement = insertParameters(statement, parameters);
        ResultSet rs = statement.executeQuery();

        return rs;
    }
}
