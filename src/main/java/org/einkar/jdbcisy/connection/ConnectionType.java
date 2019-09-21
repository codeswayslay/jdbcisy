/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.einkar.jdbcisy.connection;

/**
 *
 * @author akinw
 */
public enum ConnectionType {
    MYSQL("Connection type for MySql database"),
    ORACLE("Connection type for Oracle database"),
    SQL_SERVER("Connection type for Microsoft Sql Server database"),
    DATASOURCE("Connection type for Application server datasource through use of JNDI");
    
    private final String description;

    private ConnectionType(String description) {
        this.description = description;
    }    

    public String getDescription() {
        return description;
    }
}
