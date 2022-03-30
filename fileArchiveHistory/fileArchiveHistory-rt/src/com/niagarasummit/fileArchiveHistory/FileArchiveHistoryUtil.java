/*
 * Copyright 2022 Tridium, Inc. All Rights Reserved.
 */

package com.niagarasummit.fileArchiveHistory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import javax.baja.collection.BITable;
import javax.baja.driver.BDevice;
import javax.baja.driver.file.BIFileDevice;
import javax.baja.file.BIFile;
import javax.baja.history.BBooleanTrendRecord;
import javax.baja.history.BHistoryConfig;
import javax.baja.history.BHistoryId;
import javax.baja.history.BHistoryRecord;
import javax.baja.history.BNumericTrendRecord;
import javax.baja.history.BStringTrendRecord;
import javax.baja.naming.BOrd;
import javax.baja.status.BStatus;
import javax.baja.sys.BAbsTime;
import javax.baja.sys.BajaException;
import javax.baja.sys.Cursor;
import javax.baja.sys.Type;
import javax.baja.timezone.BTimeZone;

import com.tridium.bql.expression.Pattern;
import com.tridium.driver.file.BFileDevice;
import com.tridium.driver.file.BFileNetwork;
import com.tridium.driver.file.history.BCsvFileImport;
import com.tridium.driver.file.history.BDelimitedFileImport;
import com.tridium.driver.file.history.BFileHistoryDeviceExt;
import com.tridium.driver.file.util.CsvParser;
import com.tridium.util.ComponentTreeCursor;

/**
 * Requiring a Niagara 4.11 (or later) environment, this utility
 * class provides convenience methods to support the
 * {@link BFileArchiveHistoryProvider} example implementation, which
 * retrieves archive history data from files (such as CSV files).
 *
 * PLEASE NOTE THAT THIS CLASS IS INTENDED TO BE AN EXAMPLE IMPLEMENTATION ONLY!
 * IT IS NOT MEANT FOR PRODUCTION USE SINCE IT IS NOT OPTIMIZED IN TERMS OF MEMORY AND
 * PERFORMANCE, AND IT ALSO RELIES ON SOME NON-PUBLIC NIAGARA APIs.
 *
 * @author Scott Hoye on 02/17/2022
 * @since Niagara 4.11
 */
public final class FileArchiveHistoryUtil
{
  /**
   * Private constructor for this utility class
   * since all of the public methods are static.
   */
  private FileArchiveHistoryUtil()
  {
  }

//region Public Utility

  /**
   * Look for a matching {@link BDelimitedFileImport} descriptor in the
   * local station's {@link BFileNetwork} (if such a network exists).
   * If one is found that is configured for a History ID that matches
   * the one in the given {@link BHistoryConfig} parameter, then
   * return it wrapped in an {@link Optional}. If one is not found,
   * {@link Optional#empty()} is returned instead. If multiple matches
   * are found, the first enabled one is returned, otherwise the first
   * disabled one is returned.
   */
  public static Optional<BDelimitedFileImport> findFileImportForHistory(BHistoryConfig historyConfig)
  {
    // Use a BQL query against the local station to find an installed BFileNetwork (if one exists)
    BFileNetwork fileNetwork = null;
    BOrd findFileNetworkBql = BOrd.make("station:|slot:/|bql:select * from driver:FileNetwork stop");
    @SuppressWarnings("unchecked")
    BITable<BFileNetwork> results = (BITable<BFileNetwork>)findFileNetworkBql.get();

    // A BQL query resolves to a BITable, so use its cursor to get the first result (if it exists)
    try (Cursor<BFileNetwork> result = results.cursor())
    {
      if (result.next())
      {
        fileNetwork = result.get();
      }
    }

    if (fileNetwork == null)
    {
      // No BFileNetwork is installed, so return empty
      return Optional.empty();
    }

    // If a BFileNetwork was found, look for the first matching BDelimitedFileImport instance under
    // it that is enabled and configured for a History ID that matches the one in historyConfig.
    BHistoryId historyId = historyConfig.getId();
    BDelimitedFileImport firstDisabledFileImport = null;
    for (BDevice device : fileNetwork.getBDeviceList())
    {
      BFileDevice fileDevice = (BFileDevice) device;
      BFileHistoryDeviceExt fileHistoryDeviceExt = fileDevice.getHistories();
      try (ComponentTreeCursor cursor = new ComponentTreeCursor(fileHistoryDeviceExt, null))
      {
        while (cursor.next(BDelimitedFileImport.class))
        {
          BDelimitedFileImport fileImport = (BDelimitedFileImport) cursor.get();
          if (historyId.equals(fileImport.getHistoryId()))
          {
            if (fileImport.getEnabled())
            {
              // A match was found!
              return Optional.of(fileImport);
            }
            else if (firstDisabledFileImport == null)
            {
              // Keep track of the first disabled match, as we may need to fall
              // back to using it if we can't find an enabled match instead
              firstDisabledFileImport = fileImport;
            }
          }
        }
      }
    }

    // If we get here, either Optional.empty() will be returned, or
    // the first disabled matching descriptor will be used
    return Optional.ofNullable(firstDisabledFileImport);
  }

  /**
   * Parse and return a List of all sorted history records read from a file specified in the given
   * {@link BDelimitedFileImport} argument. The history records returned in the list will be
   * sorted by timestamp according to the given 'descending' argument.
   *
   * @param historyConfig The history configuration, used to determine the history record type of
   *                      the result as well as the timezone to use for history record timestamps.
   * @param fileImport The file import descriptor whose configured file is read and parsed.
   * @param descending Determines whether the List of history records will be sorted by timestamp
   *                   ascending order (false) or timestamp descending order (true).
   * @return A List of all sorted history records read from the file.
   * @throws BajaException if a failure occurs while parsing history records from the file
   */
  public static List<BHistoryRecord> readAndSortAllRecordsFromFile(BHistoryConfig historyConfig,
                                                                   BDelimitedFileImport fileImport,
                                                                   boolean descending)
    throws BajaException
  {
    // Get the requested history record type and timezone from the given historyConfig argument
    Type recordType = historyConfig.getRecordType().getResolvedType();
    BTimeZone timeZone = historyConfig.getTimeZone();
    if (timeZone.isNull())
    {
      timeZone = BTimeZone.getLocal();
    }

    // currentLine keeps track of the current file line number being parsed
    // (it also allows any parsing errors to include the faulty line number)
    int currentLine = 0;

    // Initialize an appropriate delimited file parser instance based on the file type
    try (IDelimitedFileParser fileParser = IDelimitedFileParser.makeDelimitedFileParser(fileImport))
    {
      // Retrieve configuration from the file import descriptor to further inform parsing
      // history records from the file
      SimpleDateFormat dateFormat = new SimpleDateFormat(fileImport.getTimestampFormat());
      int startIndex = fileImport.getRowStart();
      int endIndex = fileImport.getRowEnd();
      int timeCol = fileImport.getTimestampColumnIndex();
      int valCol = fileImport.getValueColumnIndex();
      int statusCol = fileImport.getStatusColumnIndex();
      int idCol = fileImport.getIdentifierColumnIndex();
      String idPattern = fileImport.getIdentifierPattern();
      boolean checkIdentifier = idCol >= 0 && !idPattern.isEmpty();
      Pattern rowId = checkIdentifier ? new Pattern(idPattern) : null;
      ParsePosition parsePosition = new ParsePosition(0);

      // Parse history records from each applicable line of the file and stash the records in a List
      List<BHistoryRecord> parsedRecords = new ArrayList<>();
      String[] lineData;
      while((lineData = fileParser.parseLine()) != null)
      {
        // Break out of the loop immediately if we've reached the end of the valid line range
        if (endIndex >= 0 && currentLine > endIndex)
        {
          break;
        }

        // If the loop hasn't reached the start of the valid line range yet, or
        // if a row identifier is configured on the file import descriptor for filtering valid
        // rows and the line doesn't meet the criteria, continue to the next line
        if (currentLine < startIndex ||
            checkIdentifier && !rowId.isMatch(lineData[idCol]))
        {
          currentLine++;
          continue;
        }

        // It's a valid line, so parse a history record's timestamp, value, and (optionally) status
        // from the current line
        BAbsTime timestamp;
        if (timeCol >= 0)
        {
          parsePosition.setIndex(0);
          parsePosition.setErrorIndex(-1);
          timestamp = BAbsTime.make(dateFormat.parse(lineData[timeCol], parsePosition).getTime(), timeZone);
        }
        else
        {
          timestamp = BAbsTime.now();
        }

        String value = lineData[valCol];
        BStatus status = statusCol >= 0 ? BStatus.make(Integer.parseInt(lineData[statusCol])) : BStatus.ok;

        // Create the history record and add it to the List
        BHistoryRecord historyRecord = makeHistoryRecord(recordType, timestamp, value, status);
        if (historyRecord != null)
        {
          parsedRecords.add(historyRecord);
        }

        // Advance to the next line
        currentLine++;
      }

      // Before returning the List of parsed history records, sort it according to the requested
      // timestamp order
      parsedRecords.sort(HistoryRecordComparator.makeHistoryRecordComparator(descending));
      return parsedRecords;
    }
    catch(Exception e)
    {
      // currentLine is counting lines in the file as they are being parsed, but it is zero-based.
      // For notifying the caller of errors parsing the file, convert it to a one-based file line
      // number
      currentLine++;
      throw new BajaException("Failed to parse history record on line " +
        currentLine + " of file " + fileImport.getFile(), e);
    }
  }

//endregion Public Utility

//region Private Utility

  /**
   * Convenience method to create and return a new {@link BHistoryRecord} of the
   * given type with the given timestamp, value (parsed from String form), and status.
   */
  private static BHistoryRecord makeHistoryRecord(Type recordType, BAbsTime timestamp,
                                                  String valueString, BStatus status)
  {
    // For the Boolean Record Type, we use custom parsing because it's possible that
    // the String could be either "true" or "false", but it could also be in Integer String form.
    // For the Integer case, zero is assumed to be false and anything else (e.g. 1) is true.
    if (recordType.is(BBooleanTrendRecord.TYPE))
    {
      boolean boolVal = false;
      if ("true".equalsIgnoreCase(valueString))
      {
        boolVal = true;
      }
      else if (!"false".equalsIgnoreCase(valueString))
      {
        try
        {
          boolVal = Integer.parseInt(valueString) != 0;
        }
        catch (NumberFormatException ignore)
        {
        }
      }
      BBooleanTrendRecord record = (BBooleanTrendRecord) recordType.getInstance();
      return record.set(timestamp, boolVal, status);
    }

    // Numeric Record Type handling
    if (recordType.is(BNumericTrendRecord.TYPE))
    {
      BNumericTrendRecord record = (BNumericTrendRecord) recordType.getInstance();
      return record.set(timestamp, Double.parseDouble(valueString), status);
    }

    // String Record Type handling
    if (recordType.is(BStringTrendRecord.TYPE))
    {
      BStringTrendRecord record = (BStringTrendRecord) recordType.getInstance();
      return record.set(timestamp, valueString, status);
    }

    // Any other types are not supported
    return null;
  }

  /**
   * Private inner interface for Delimited File Parsers
   */
  private interface IDelimitedFileParser extends AutoCloseable
  {
    /**
     * Read the next line from the file and return its parsed contents in String array form.
     * If the end of the file has been reached, this method should return null.
     */
    String[] parseLine() throws IOException;

    /**
     * Close the file parser and release any internal resources used.
     */
    @Override
    void close() throws IOException;

    /**
     * For the given {@link BDelimitedFileImport} descriptor, return a proper, concrete
     * File Parser instance to use for parsing the descriptor's file.
     */
    static IDelimitedFileParser makeDelimitedFileParser(BDelimitedFileImport fileImport)
      throws IOException
    {
      // All File Parsers utilize a Reader, so go ahead and create one for the target file
      BIFile file = ((BIFileDevice)fileImport.getDevice()).resolveFile(fileImport.getFile());
      BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()));

      if (fileImport.getType().is(BCsvFileImport.TYPE))
      {
        // CSV file types have special handling, so they have a special parser
        return new CsvFileParser(reader);
      }
      else
      {
        // Generic delimited file types all use this parser
        return new DelimitedFileParser(reader, fileImport.getDelimiter());
      }
    }
  }

  /**
   * Private CSV File Parser implementation
   */
  private static class CsvFileParser
    implements IDelimitedFileParser
  {
    CsvFileParser(BufferedReader reader)
    {
      this.reader = reader;
      csvParser = new CsvParser(reader);
    }

    @Override
    public String[] parseLine()
      throws IOException
    {
      return csvParser.readLine();
    }

    @Override
    public void close()
      throws IOException
    {
      reader.close();
    }

    private final BufferedReader reader;
    private final CsvParser csvParser;
  }

  /**
   * Private Generic, Delimited File Parser implementation
   */
  private static class DelimitedFileParser
    implements IDelimitedFileParser
  {
    DelimitedFileParser(BufferedReader fileReader, String delimiter)
    {
      this.fileReader = fileReader;
      this.delimiter = delimiter;
    }

    @Override
    public String[] parseLine()
      throws IOException
    {
      String line = fileReader.readLine();
      if (line == null)
      {
        return null;
      }

      return line.split(delimiter);
    }

    @Override
    public void close()
      throws IOException
    {
      fileReader.close();
    }

    private final BufferedReader fileReader;
    private final String delimiter;
  }

  /**
   * Private History Record Comparator used for sorting by timestamp ascending or descending order.
   */
  private static class HistoryRecordComparator
    implements Comparator<BHistoryRecord>
  {
    /**
     * Private constructor since static (shared) instances are used. See
     * {@link #makeHistoryRecordComparator(boolean)}.
     */
    private HistoryRecordComparator(boolean descending)
    {
      this.descending = descending;
    }

    /**
     * Compares the given history records based on their timestamps.
     */
    @Override
    public int compare(BHistoryRecord record1, BHistoryRecord record2)
    {
      BAbsTime timestamp1 = record1.getTimestamp();
      BAbsTime timestamp2 = record2.getTimestamp();
      int result = timestamp1.compareTo(timestamp2);
      if (descending)
      {
        result = -result;
      }
      return result;
    }

    /**
     * Factory that returns a HistoryRecordComparator based on the given timestamp sorting order
     */
    public static HistoryRecordComparator makeHistoryRecordComparator(boolean descending)
    {
      return descending ? DESCENDING_COMPARATOR : ASCENDING_COMPARATOR;
    }

    private final boolean descending;

    // Use shared (immutable) instances for the ascending and descending case
    private static final HistoryRecordComparator ASCENDING_COMPARATOR =
      new HistoryRecordComparator(false);
    private static final HistoryRecordComparator DESCENDING_COMPARATOR =
      new HistoryRecordComparator(true);
  }

//endregion Private Utility

}
