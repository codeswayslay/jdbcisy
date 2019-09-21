/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jdbcisy.transformer;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import com.itextpdf.text.Document;
import com.itextpdf.text.Element;
import com.itextpdf.text.Font;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.Phrase;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfWriter;
import java.io.Writer;
import java.nio.file.Files;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/**
 *
 * @author Akinwale Agbaje
 */
public class Transformer {

    private static final Logger logger = LogManager.getLogger(Transformer.class);
    
    public static boolean exportToCsv(String filename, String pathToFile, List<String> headers, List<Object[]> body) throws Exception {
        if (filename == null || filename.trim().isEmpty()) {
            logger.info("The file name must be provided");
            throw new Exception("The file name must be provided");
        }

        if (pathToFile == null || pathToFile.trim().isEmpty()) {
            logger.info("The folder path must be provided. Cannot create file directly in root directory");
            throw new Exception("The folder path must be provided. Cannot create file directly in root directory");
        }
        
        Path path = Paths.get(pathToFile, filename);
        
        File excelFile = path.toFile();
        if (excelFile.getParentFile() == null) {
            logger.info("no directory specified");
        } else {
            if (!excelFile.getParentFile().exists()) {
                logger.info("directory does not exist. It will be created");
                excelFile.getParentFile().mkdirs();
            }
        }
        
        String extension = "";
        int index = excelFile.getName().lastIndexOf('.');
        if (index > 0) {
            extension = excelFile.getName().substring(index + 1);
        }
        
        if (!extension.equalsIgnoreCase("xlsx")) {
            logger.info("file with incorrect extension provided. Extension is {} when it should be xlsx", extension);
            throw new Exception("file with incorrect extension provided. Extension is " + extension + " when it should be xlsx");
        }
        
        Writer writer = Files.newBufferedWriter(path);
        CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(headers.toArray(new String[headers.size()])));
        
        for (Object[] objArray : body) {
            csvPrinter.printRecords(objArray);
        }
        
        csvPrinter.flush();
        
        return true;
    }

    /**
     * Exports data to an excel file.
     *
     * @param filename the file name to give the exported file
     * @param pathToFile the folder path where the file will be deposited
     * @param headers the headers in the excel file
     * @param body the body besides the headers, in the excel file
     * @return true, if export is successful
     * @throws java.io.FileNotFoundException
     */
    public static boolean exportToExcel(String filename, String pathToFile, List<String> headers, List<Object[]> body) throws Exception {
        if (filename == null || filename.trim().isEmpty()) {
            logger.info("The file name must be provided");
            throw new Exception("The file name must be provided");
        }

        if (pathToFile == null || pathToFile.trim().isEmpty()) {
            logger.info("The folder path must be provided. Cannot create file directly in root directory");
            throw new Exception("The folder path must be provided. Cannot create file directly in root directory");
        }

        XSSFWorkbook workbook = new XSSFWorkbook();
        XSSFSheet sheet = workbook.createSheet("result");

        XSSFFont font = workbook.createFont();
        font.setBold(true);

        int rownumber = 0;
        Row row = sheet.createRow(rownumber);

        int headerCellNo = 0;
        for (String header : headers) {
            XSSFRichTextString richString = new XSSFRichTextString(header);
            richString.applyFont(font);
            Cell cell = row.createCell(headerCellNo++);
            cell.setCellValue(richString);
        }

        for (Object[] objArray : body) {
            rownumber++;
            row = sheet.createRow(rownumber);

            Object[] data = objArray;
            int bodyCellNo = 0;

            //this first cell is created for the S/N, which begins before all other data cells per row
            Cell cell = row.createCell(bodyCellNo++);
            cell.setCellValue(rownumber);
            for (Object obj : data) {
                cell = row.createCell(bodyCellNo++);
                if (obj instanceof Date) {
                    sqlDateToMsExcelFormat(workbook, cell, obj);
                } else if (obj instanceof Boolean) {
                    cell.setCellValue((Boolean) obj);
                } else if (obj instanceof String) {
                    cell.setCellValue((String) obj);
                } else if (obj instanceof Double) {
                    cell.setCellValue((Double) obj);
                } else if (obj instanceof Integer) {
                    cell.setCellValue((Integer) obj);
                } else if (obj instanceof Long) {
                    cell.setCellValue((Long) obj);
                } else if (obj instanceof Byte) {
                    cell.setCellValue((Byte) obj);
                } else {
                    cell.setCellValue(String.valueOf(obj));
                }
            }
        }

        Path path = Paths.get(pathToFile, filename);

        File excelFile = path.toFile();
        if (excelFile.getParentFile() == null) {
            logger.info("no directory specified");
        } else {
            if (!excelFile.getParentFile().exists()) {
                logger.info("directory does not exist. It will be created");
                excelFile.getParentFile().mkdirs();
            }
        }

        String extension = "";
        int index = excelFile.getName().lastIndexOf('.');
        if (index > 0) {
            extension = excelFile.getName().substring(index + 1);
        }

        if (!extension.equalsIgnoreCase("xlsx")) {
            logger.info("file with incorrect extension provided. Extension is {} when it should be xlsx", extension);
            throw new Exception("file with incorrect extension provided. Extension is " + extension + " when it should be xlsx");
        }

        try (FileOutputStream out = new FileOutputStream(excelFile)) {
            workbook.write(out);
        }

        return true;
    }

    /**
     * Exports data to an excel file.
     *
     * @param filename the file name to give the exported file
     * @param pathToFile the folder path where the file will be deposited
     * @param headers the headers in the excel file
     * @param body the body besides the headers, in the excel file
     * @return true, if export is successful
     * @throws java.io.FileNotFoundException
     */
    public static boolean exportLargeDataToExcel(String filename, String pathToFile, List<String> headers, List<Object[]> body) throws Exception {
        if (filename == null || filename.trim().isEmpty()) {
            logger.info("The file name must be provided");
            throw new Exception("The file name must be provided");
        }

        if (pathToFile == null || pathToFile.trim().isEmpty()) {
            logger.info("The folder path must be provided. Cannot create file directly in root directory");
            throw new Exception("The folder path must be provided. Cannot create file directly in root directory");
        }

        SXSSFWorkbook workbook = new SXSSFWorkbook();
        Sheet sheet = workbook.createSheet("result");

        XSSFFont font = (XSSFFont) sheet.getWorkbook().createFont();
        font.setBold(true);
        font.setFontHeightInPoints((short) 14);

        int rownumber = 0;
        Row row = sheet.createRow(rownumber);

        int headerCellNo = 0;
        for (String header : headers) {
            XSSFRichTextString richString = new XSSFRichTextString(header);
            richString.applyFont(font);
            Cell cell = row.createCell(headerCellNo++);
            cell.setCellValue(richString);
        }

        for (Object[] objArray : body) {
            rownumber++;
            row = sheet.createRow(rownumber);

            Object[] data = objArray;
            int bodyCellNo = 0;

            //this first cell is created for the S/N, which begins before all other data cells per row
            Cell cell = row.createCell(bodyCellNo++);
            cell.setCellValue(rownumber);
            for (Object obj : data) {
                cell = row.createCell(bodyCellNo++);
                if (obj instanceof Date) {
                    sqlDateToLargeMsExcelFormat(workbook, cell, obj);
                } else if (obj instanceof Boolean) {
                    cell.setCellValue((Boolean) obj);
                } else if (obj instanceof String) {
                    cell.setCellValue((String) obj);
                } else if (obj instanceof Double) {
                    cell.setCellValue((Double) obj);
                } else if (obj instanceof Integer) {
                    cell.setCellValue((Integer) obj);
                } else if (obj instanceof Long) {
                    cell.setCellValue((Long) obj);
                } else if (obj instanceof Byte) {
                    cell.setCellValue((Byte) obj);
                } else {
                    cell.setCellValue(String.valueOf(obj));
                }
            }
        }

        Path path = Paths.get(pathToFile, filename);

        File excelFile = path.toFile();
        if (excelFile.getParentFile() == null) {
            logger.info("no directory specified");
        } else {
            if (!excelFile.getParentFile().exists()) {
                logger.info("directory does not exist. It will be created");
                excelFile.getParentFile().mkdirs();
            }
        }

        String extension = "";
        int index = excelFile.getName().lastIndexOf('.');
        if (index > 0) {
            extension = excelFile.getName().substring(index + 1);
        }

        if (!extension.equalsIgnoreCase("xlsx")) {
            logger.info("file with incorrect extension provided. Extension is {} when it should be xlsx", extension);
            throw new Exception("file with incorrect extension provided. Extension is " + extension + " when it should be xlsx");
        }

        try (FileOutputStream out = new FileOutputStream(excelFile)) {
            workbook.write(out);
        }

        return true;
    }

    protected static void sqlDateToMsExcelFormat(XSSFWorkbook workbook, Cell cell, Object obj) {
        CellStyle cell_style = workbook.createCellStyle();
        CreationHelper style_helper = workbook.getCreationHelper();
        cell_style.setDataFormat(style_helper.createDataFormat().getFormat("yyyy-MM-dd"));
        cell.setCellValue((Date) obj);
        cell.setCellStyle(cell_style);
    }

    protected static void sqlDateToLargeMsExcelFormat(SXSSFWorkbook workbook, Cell cell, Object obj) {
        CellStyle cell_style = workbook.createCellStyle();
        CreationHelper style_helper = workbook.getCreationHelper();
        cell_style.setDataFormat(style_helper.createDataFormat().getFormat("yyyy-MM-dd"));
        cell.setCellValue((Date) obj);
        cell.setCellStyle(cell_style);
    }

    /**
     * Exports data to pdf file.
     *
     * @param title
     * @param fileName the file name to give the exported file
     * @param pathToFile the folder path where the file will be deposited
     * @param headers the headers in the excel file
     * @param body the body besides the headers, in the excel file
     * @return true, if export is successful
     * @throws java.io.FileNotFoundException
     */
    public static boolean exportToPDF(String title, String fileName, String pathToFile, List<String> headers, List<Object[]> body) throws Exception {
        logger.info("processing pdf file generation..");
        if (fileName == null || fileName.trim().isEmpty()) {
            logger.info("The file name must be provided");
            throw new Exception("The file name must be provided");
        }

        if (pathToFile == null || pathToFile.trim().isEmpty()) {
            logger.info("The folder path must be provided. Cannot create file directly in root directory");
            throw new Exception("The folder path must be provided. Cannot create file directly in root directory");
        }

        try {

            Path path = Paths.get(pathToFile, fileName);
            
            File pdfFile = path.toFile();
            if (pdfFile.getParentFile() == null) {
                logger.info("no directory specified");
            } else {
                if (!pdfFile.getParentFile().exists()) {
                    logger.info("directory does not exist. It will be created");
                    pdfFile.getParentFile().mkdirs();
                }
            }
            
            String extension = "";
            int index = pdfFile.getName().lastIndexOf('.');
            if (index > 0) {
                extension = pdfFile.getName().substring(index + 1);
            }
            
            if (!extension.equalsIgnoreCase("pdf")) {
                logger.info("file with incorrect extension provided. Extension is {} when it should be pdf", extension);
                throw new Exception("file with incorrect extension provided. Extension is " + extension + " when it should be pdf");
            }

            Document doc = new Document();
            PdfWriter.getInstance(doc, new FileOutputStream(pathToFile + fileName));

            Font font1 = new Font(Font.FontFamily.HELVETICA, 12, Font.BOLD);
            Font font2 = new Font(Font.FontFamily.HELVETICA, 12, Font.NORMAL);

            doc.open();

            Paragraph para_title = new Paragraph(title, new Font(Font.FontFamily.HELVETICA, 18, Font.BOLD));
            para_title.setAlignment(Element.ALIGN_CENTER);
            doc.add(para_title);

            Paragraph p1 = new Paragraph(" ");
            doc.add(p1);

            PdfPTable table = new PdfPTable(headers.size());
            table.setWidthPercentage(100);
            int rownumber = 0;
            for (String h : headers) {
                table.addCell(new Phrase(h, font1));
            }

            logger.info("generating file now...");
            for (Object[] objArray : body) {
                rownumber++;
                table.addCell(new Phrase(String.valueOf(rownumber), font1));

                Object[] data = objArray;
                for (Object obj : data) {
                    table.addCell(new Phrase((String.valueOf(obj)), font2));
                }

            }

            doc.add(table);
            doc.close();
            logger.info("Finished generating file.");
        } catch (Exception ex) {
            logger.info("Error generating file.");
            logger.error("An error occurred while trying to generate pdf file.", ex);
        }
        return true;
    }
}
