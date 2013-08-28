/* 
 * Copyright (C) 2013 Lisa Park, Inc. (www.lisa-park.net)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.lisapark.octopus.util.gss;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
import com.google.gdata.client.spreadsheet.*;
import com.google.gdata.data.Link;
import com.google.gdata.data.batch.BatchOperationType;
import com.google.gdata.data.batch.BatchStatus;
import com.google.gdata.data.batch.BatchUtils;
import com.google.gdata.data.spreadsheet.*;
import com.google.gdata.util.*;

import java.io.IOException;
import java.net.*;
import java.util.*;
import org.openide.util.Exceptions;

public class GssListUpdateUtils {

    public static void main(String[] args)
            throws AuthenticationException, MalformedURLException, IOException, ServiceException, URISyntaxException {

        String userEmail = "demo@lisa-park.com";
        String password = "isasdemo";
        String spreadSheetName = "demoForecast_USFutures15";
        String workSheetName = "SourceData";

        int minRow = 2;
        int maxRow = 99;

        int minCol = 6;
        int maxCol = 6;
        SpreadsheetService service = getService(userEmail, password);

        batchUpdate(service, spreadSheetName, workSheetName, minRow, maxRow, minCol, maxCol);
        
//        updateSample(service, spreadSheetName, workSheetName, minRow, minCol, maxCol);
    }

    public static void batchUpdate(SpreadsheetService service, String spreadSheetName, String workSheetName,
            int minRow, int maxRow, int minCol, int maxCol)
            throws AuthenticationException, MalformedURLException, IOException, ServiceException, URISyntaxException {

        long startTime = System.currentTimeMillis();

        FeedURLFactory factory = FeedURLFactory.getDefault();

        // Make a request to the API and get all spreadsheets.
        SpreadsheetFeed feed = service.getFeed(factory.getSpreadsheetsFeedUrl(),
                SpreadsheetFeed.class);
        List<SpreadsheetEntry> spreadsheets = feed.getEntries();

        int spreadsheetIndex = getSpreadsheetIndex(spreadsheets, spreadSheetName);

        if (spreadsheets.size() == 0) {
            // TODO: There were no spreadsheets, act accordingly.
        }

        SpreadsheetEntry spreadsheet = spreadsheets.get(spreadsheetIndex);
        System.out.println(spreadsheet.getTitle().getPlainText());

        WorksheetFeed worksheetFeed = service.getFeed(
                spreadsheet.getWorksheetFeedUrl(), WorksheetFeed.class);
        List<WorksheetEntry> worksheets = worksheetFeed.getEntries();
        
        int workSheetIndex = getWorksheetIndex(worksheets, workSheetName);
        WorksheetEntry worksheet = worksheets.get(workSheetIndex);

        // Fetch column 4, and every row after row 1.
        URL cellFeedUrl = new URI(worksheet.getCellFeedUrl().toString()
                + "?min-row="
                + minRow + "&min-col="
                + minCol
                + "&max-col="
                + maxCol).toURL();
        CellFeed cellFeed = service.getFeed(cellFeedUrl, CellFeed.class);

        List<CellAddress> cellAddrs = new ArrayList<CellAddress>();


        // Build list of cell addresses to be filled in
        for (int row = minRow; row <= maxRow; ++row) {
            for (int col = minCol; col <= maxCol; ++col) {
                cellAddrs.add(new CellAddress(row, col));
            }
        }

        // Prepare the update
        // getCellEntryMap is what makes the update fast.
        Map<String, CellEntry> cellEntries = getCellEntryMap(service, cellFeedUrl, cellAddrs);

        CellFeed batchRequest = new CellFeed();
        for (CellAddress cellAddr : cellAddrs) {
            URL entryUrl = new URL(cellFeedUrl.toString() + "/" + cellAddr.idString);
            CellEntry batchEntry = new CellEntry(cellEntries.get(cellAddr.idString));
            batchEntry.changeInputValueLocal(cellAddr.idString);
            BatchUtils.setBatchId(batchEntry, cellAddr.idString);
            BatchUtils.setBatchOperationType(batchEntry, BatchOperationType.UPDATE);
            batchRequest.getEntries().add(batchEntry);
        }

        // Submit the update
        Link batchLink = cellFeed.getLink(Link.Rel.FEED_BATCH, Link.Type.ATOM);
        CellFeed batchResponse = service.batch(new URL(batchLink.getHref()), batchRequest);

        // Check the results
        boolean isSuccess = true;
        for (CellEntry entry : batchResponse.getEntries()) {
            String batchId = BatchUtils.getBatchId(entry);
            if (!BatchUtils.isSuccess(entry)) {
                isSuccess = false;
                BatchStatus status = BatchUtils.getBatchStatus(entry);
                System.out.printf("%s failed (%s) %s", batchId, status.getReason(), status.getContent());
            }
        }

        System.out.println(isSuccess ? "\nBatch operations successful." : "\nBatch operations failed");
        System.out.printf("\n%s ms elapsed\n", System.currentTimeMillis() - startTime);
    }
    
    /**
     * Returns spreadsheet index by SpreadSheet name
     *
     * @param spreadsheets
     * @param spreadSheetName
     * @return spreadsheet index
     */
    private static int getSpreadsheetIndex(List<SpreadsheetEntry> spreadsheets, String spreadSheetName) {
        int index = 0;

        String itemNameTrimmed = spreadSheetName.trim();
        for (SpreadsheetEntry item : spreadsheets) {
            if (item.getTitle().getPlainText().trim().equalsIgnoreCase(itemNameTrimmed)) {
                break;
            }
            index++;
        }
        return index;
    }

     /**
     * Returns index from worksheet list by workSheetName
     * 
     * @param worksheets
     * @param workSheetName
     * @return workSheet index
     */
    private static int getWorksheetIndex(List<WorksheetEntry> worksheets, String workSheetName) {
        int index = 0;
        
        String itemNameTrimmed = workSheetName.trim();
        for(WorksheetEntry item : worksheets){
            if(item.getTitle().getPlainText().trim().equalsIgnoreCase(itemNameTrimmed)) break;
            index++;
        }
        return index;
    } 
    
    public static SpreadsheetService getService(String userEmail, String password) {
        SpreadsheetService service =
                new SpreadsheetService("org.lisapark.octopus.util.gss.GssListUpdateUtils");
        try {
            // Authenticate
            service.setUserCredentials(userEmail, password);
        } catch (AuthenticationException ex) {
            Exceptions.printStackTrace(ex);
        }
        return service;
    }

    private static void updateSample(SpreadsheetService service, String spreadSheetName, String workSheetName, 
            int minRow, int minCol, int maxCol) 
            throws ServiceException, MalformedURLException, IOException, URISyntaxException {
       
        FeedURLFactory factory = FeedURLFactory.getDefault();
        // Make a request to the API and get all spreadsheets.
        SpreadsheetFeed feed = service.getFeed(factory.getSpreadsheetsFeedUrl(),
                SpreadsheetFeed.class);
        List<SpreadsheetEntry> spreadSheets = feed.getEntries();

        int spreadSheetIndex = getSpreadsheetIndex(spreadSheets, spreadSheetName);

        if (spreadSheets.size() == 0) {
            // TODO: There were no spreadsheets, act accordingly.
        }

        // TODO: Choose a spreadsheet more intelligently based on your
        // app's needs.
        SpreadsheetEntry spreadsheet = spreadSheets.get(spreadSheetIndex);
        System.out.println(spreadsheet.getTitle().getPlainText());

        // Get the first worksheet of the first spreadsheet.
        // TODO: Choose a worksheet more intelligently based on your
        // app's needs.
        WorksheetFeed worksheetFeed = service.getFeed(
                spreadsheet.getWorksheetFeedUrl(), WorksheetFeed.class);
        List<WorksheetEntry> workSheets = worksheetFeed.getEntries();
        
        int workSheetIndex = getWorksheetIndex(workSheets, workSheetName);
//        int spreadSheetIndex = 
        WorksheetEntry worksheet = workSheets.get(workSheetIndex);

        // Fetch column 4, and every row after row 1.
        URL cellFeedUrl = new URI(worksheet.getCellFeedUrl().toString() + "?min-row="
                + minRow + "&min-col=4"
                + minCol + "&max-col="
                + maxCol).toURL();
        CellFeed cellFeed = service.getFeed(cellFeedUrl, CellFeed.class);

        // Iterate through each cell, printing its value.
        for (CellEntry cell : cellFeed.getEntries()) {
            // Print the cell's address in A1 notation
            System.out.print(cell.getTitle().getPlainText() + "\t");
            // Print the cell's address in R1C1 notation
            System.out.print(cell.getId().substring(cell.getId().lastIndexOf('/') + 1) + "\t");
            // Print the cell's formula or text value
            System.out.print(cell.getCell().getInputValue() + "\t");
            // Print the cell's calculated value if the cell's value is numeric
            // Prints empty string if cell's value is not numeric
            System.out.print(cell.getCell().getNumericValue() + "\t");
            // Print the cell's displayed value (useful if the cell has a formula)
            System.out.println(cell.getCell().getValue() + "\t");
        }
    }

    /**
     * A basic struct to store cell row/column information and the associated
     * RnCn identifier.
     */
    private static class CellAddress {

        public final int row;
        public final int col;
        public final String idString;

        /**
         * Constructs a CellAddress representing the specified {@code row} and
         * {@code col}. The idString will be set in 'RnCn' notation.
         */
        public CellAddress(int row, int col) {
            this.row = row;
            this.col = col;
            this.idString = String.format("R%sC%s", row, col);
        }
    }

    /**
     * Connects to the specified {@link SpreadsheetService} and uses a batch
     * request to retrieve a {@link CellEntry} for each cell enumerated in {@code
     * cellAddrs}. Each cell entry is placed into a map keyed by its RnCn
     * identifier.
     *
     * @param ssSvc the spreadsheet service to use.
     * @param cellFeedUrl url of the cell feed.
     * @param cellAddrs list of cell addresses to be retrieved.
     * @return a map consisting of one {@link CellEntry} for each address in {@code
     *         cellAddrs}
     */
    private static Map<String, CellEntry> getCellEntryMap(
            SpreadsheetService service, URL cellFeedUrl, List<CellAddress> cellAddrs)
            throws IOException, ServiceException {
        CellFeed batchRequest = new CellFeed();
        for (CellAddress cellId : cellAddrs) {
            CellEntry batchEntry = new CellEntry(cellId.row, cellId.col, cellId.idString);
            batchEntry.setId(String.format("%s/%s", cellFeedUrl.toString(), cellId.idString));
            BatchUtils.setBatchId(batchEntry, cellId.idString);
            BatchUtils.setBatchOperationType(batchEntry, BatchOperationType.QUERY);
            batchRequest.getEntries().add(batchEntry);
        }

        CellFeed cellFeed = service.getFeed(cellFeedUrl, CellFeed.class);
        CellFeed queryBatchResponse =
                service.batch(new URL(cellFeed.getLink(Link.Rel.FEED_BATCH, Link.Type.ATOM).getHref()),
                batchRequest);

        Map<String, CellEntry> cellEntryMap = new HashMap<String, CellEntry>(cellAddrs.size());
        for (CellEntry entry : queryBatchResponse.getEntries()) {
            cellEntryMap.put(BatchUtils.getBatchId(entry), entry);
            System.out.printf("batch %s {CellEntry: id=%s editLink=%s inputValue=%s\n",
                    BatchUtils.getBatchId(entry), entry.getId(), entry.getEditLink().getHref(),
                    entry.getCell().getInputValue());
        }

        return cellEntryMap;
    }
}