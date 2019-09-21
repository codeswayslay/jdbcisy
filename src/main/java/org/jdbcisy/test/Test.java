/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jdbcisy.test;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.jdbcisy.DatabaseEngine;
import org.jdbcisy.transformer.Transformer;
import org.jdbcisy.properties.InMemoryQueryProperties;

/**
 *
 * @author ahmad.gbadamosi
 */
public class Test {

    private final InMemoryQueryProperties inMemProp = InMemoryQueryProperties.getInstance();

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        String sql = "monthly-update-summary";
        String sql_escape = "{call " + sql + "(?, ?, ?)}";
        System.out.println(sql_escape);
        new Test().runEngineUtilisation();
    }

    public void runEngineUtilisation() throws Exception {
        System.out.println("opening connection...");
        DatabaseEngine engine = new DatabaseEngine();//this will open the connection

        String sql = inMemProp.getProperty("monthly-update-summary");
        if (sql == null) {
            System.out.println("query not found in properties file" + sql);
            engine.closeConnection();
        } else {
            System.out.println("sql from properties: " + inMemProp.getProperty("monthly-update-summary"));
            
            String date1 = "2017-03-21";
            String date2 = "2017-04-20";
            /*SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
            SimpleDateFormat user_format = new SimpleDateFormat("yyyy-MM-dd");

            Date date_1 = formatter.parse(formatter.format(user_format.parse(date1)));
            Date date_2 = formatter.parse(formatter.format(user_format.parse(date2)));

            java.sql.Date asAtDate1 = new java.sql.Date(date_1.getTime());
            java.sql.Date asAtDate2 = new java.sql.Date(date_2.getTime());*/
            
            //result set and prepared statement are closed once engine runs statement
            //commit transaction and close connection afterwards
            System.out.println("running statement...");
            //List result = engine.runStatement(sql); //, clientcompanyid
            //String sql_escape = "{call " + sql + "(" + "equity" + ", " + date1 + ", " + date2 + ")}";
            String sql_escape = "{call " + sql + "(?, ?, ?)}";
            System.out.println(sql_escape);
            List result = engine.runNonParameterizedProcedureStatement(sql_escape, "equity", date1, date2);
            System.out.println("statement finished running. Committing transaction and closing connection...");
            engine.commitTransaction();
            engine.closeConnection();

            System.out.println("transforming raw data into excel file...");
            //test should be put in if statement in case result is empty
            if (result.size() != 2) {
                System.out.println("Error: result should only contain 2 values: headers and contents lists");
            } else {
                List<String> headers = (List<String>) result.get(0);
                List<Object[]> body = (List<Object[]>) result.get(1);

                //boolean successful = Transformer.exportToExcel("test.xlsx","/etc/greenpole/reporttest/", headers, body);
                boolean successful = Transformer.exportLargeDataToExcel("monthlyBill.xlsx", "/etc/greenpole/reporttest/", headers, body);
                System.out.println("successful?: " + successful);
            }
        }
    }

    public void runEngineUtilisationForObject() throws Exception {
        System.out.println("opening connection...");
        DatabaseEngine engine = new DatabaseEngine();//this will open the connection

        String sql = inMemProp.getProperty("equity_percentage__nodate_ers");
        if (sql == null) {
            System.out.println("query not found in properties file" + sql);
            engine.closeConnection();
        } else {
            System.out.println("sql from properties: " + inMemProp.getProperty("equity_percentage__nodate_ers"));

            int clientcompanyid = 30;

            //result set and prepared statement are closed once engine runs statement
            //commit transaction and close connection afterwards
            System.out.println("running statement...");
            ResultSet result = engine.returnResultSet(sql, clientcompanyid); //, clientcompanyid
            System.out.println("statement finished running. Committing transaction and closing connection...");
            engine.commitTransaction();

            System.out.println("statement finished running, with result size: {}. Committing transaction and closing connection - [{}]" + result.getRow());
            System.out.println("transforming raw data into excel file...");
            /*List<AccountErs> percentageholder_all = new ArrayList<>();
            while (result.next()) {
                AccountErs holder_percent = new AccountErs();
                holder_percent.setAccountno(result.getInt("ACCOUNT_NO"));
                if (holder_percent.isIsequity()) {
                    holder_percent.setEquityholdings(result.getLong("HOLDINGS"));
                } else {
                    holder_percent.setMutualfundholdings(result.getDouble("HOLDINGS"));
                }
                holder_percent.setShareholdername(result.getString("HOLDER_FULL_NAME"));
                holder_percent.setAddress(result.getString("HOLDER_ADDRESS"));
                holder_percent.setPercentageholdings(result.getDouble("HOLDING_PERCENTAGE"));
                holder_percent.setShareholdertype(result.getString("HOLDER_TYPE"));

                System.out.println(holder_percent);
                percentageholder_all.add(holder_percent);
            }*/
            engine.closeConnection();
        }

    }
}
