/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.einkar.jdbcisy.sql;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author Akinwale.Agbaje
 */
public class SQLCreator {
    private static final Logger logger = LogManager.getLogger(SQLCreator.class);
    
    /**
     * Dynamically creates an SQL select statement utilising the provided columns, tables and conditions.
     * 
     * @param columns the columns to be used in the sql statement. If none is provided, a default star (*) is used
     * @param tables the tables to be used in the sql statement. No less than one must be provided.
     * @param conditions the conditions to be added after the where clause, merged with "And". 
     * Should come in the format: "condition = ?" or "condition >= ?" etc. This is an optional parameter
     * @return the fully formed SQL statement
     */
    public static String createSelectStatement(List<String> columns, List<String> tables, List<String> conditions) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        
        //process columns
        if (columns != null && columns.size() > 0) {
            String[] columns_ = columns.stream().toArray(String[]::new);
            for (int i = 0; i < columns_.length; i++) {
                sb.append(columns_[i]);
                
                //add comma after column unless column is the last one
                if (i != columns_.length - 1) {
                    sb.append(", ");
                }
            }
            sb.append(" FROM ");
        } else {
            sb.append("*").append(" FROM ");
        }
        
        //process tables
        if (tables != null && tables.size() > 0) {
            String[] tables_ = tables.stream().toArray(String[]::new);
            for (int i = 0; i < tables_.length; i++) {
                sb.append(tables_[i]);
                
                //add comma after table unless table is the last one
                if (i != tables_.length - 1) {
                    sb.append(", ");
                }
            }
        } else {
            throw new Exception("The SQL query must contain at least one table");
        }
        
        //process conditions
        if (conditions != null && conditions.size() > 0) {
            sb.append(" WHERE ");
            
            String[] conditions_ = conditions.stream().toArray(String[]::new);
            for (int i = 0; i < conditions_.length; i++) {
                sb.append(conditions_[i]);
                
                
                //ignore "AND"
                boolean ignoreAnd = false;
                int nextIndex;
                if (i + 1 <= conditions_.length - 1) {
                    nextIndex = i + 1;
                } else {
                    nextIndex = i;
                }
                
                if (nextIndex != i && (conditions_[nextIndex].toLowerCase().startsWith("having") || 
                        conditions_[nextIndex].toLowerCase().startsWith("group"))) {
                    ignoreAnd = true;
                }
                
                //add 'AND' after each condition as long 
                //as conditions are before the last
                if (i != conditions_.length - 1) {
                    if (!ignoreAnd)
                        sb.append(" AND ");
                    else
                        sb.append(" ");
                }
            }
        }
        
        logger.info("created query string: {}", sb.toString());
        return sb.toString();
    }
    
    public static void main(String[] args) throws Exception {
        List<String> columns = new ArrayList<>(), 
                tables = new ArrayList<>(), 
                conditions = new ArrayList<>();
        
        columns.add("first_name");
        columns.add("last_name");
        
        tables.add("member left join orders o on member.id = o.member_id");
        tables.add("subject");
        
        conditions.add("member.id = ?");
        conditions.add("member.id = subject.member_id");
        conditions.add("middle_name <> 'love'");
        conditions.add("(chn = ? OR phone = ?)");
        conditions.add("Having SUM(balance) > 45");
        conditions.add("Group By first_name, last_name");
        
        String sql = SQLCreator.createSelectStatement(columns, tables, conditions);
        System.out.println("sql: " + sql);
    }
}
