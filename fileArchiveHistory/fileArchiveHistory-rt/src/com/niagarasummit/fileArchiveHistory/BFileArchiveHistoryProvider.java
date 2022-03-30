/*
 * Copyright 2022 Tridium, Inc. All Rights Reserved.
 */

package com.niagarasummit.fileArchiveHistory;

import static com.niagarasummit.fileArchiveHistory.FileArchiveHistoryUtil.findFileImportForHistory;
import static com.niagarasummit.fileArchiveHistory.FileArchiveHistoryUtil.readAndSortAllRecordsFromFile;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import javax.baja.history.BHistoryConfig;
import javax.baja.history.BHistoryRecord;
import javax.baja.history.HistoryCursor;
import javax.baja.history.db.BArchiveHistoryProvider;
import javax.baja.history.db.BArchiveHistoryProviders;
import javax.baja.nre.annotations.NiagaraType;
import javax.baja.sys.BAbsTime;
import javax.baja.sys.BajaRuntimeException;
import javax.baja.sys.Context;
import javax.baja.sys.Cursor;
import javax.baja.sys.Sys;
import javax.baja.sys.Type;

import com.tridium.driver.file.BFileNetwork;
import com.tridium.driver.file.history.BDelimitedFileImport;

/**
 * Requiring a Niagara 4.11 (or later) environment, this class is an example
 * {@link BArchiveHistoryProvider} implementation that retrieves archive history data
 * from files, such as CSV files. In order to know how to parse {@link BHistoryRecord}
 * instances from a particular file, this provider will look for matching (enabled)
 * {@link BDelimitedFileImport} descriptors in the local station, which reside somewhere
 * under a {@link BFileNetwork}. When using this provider in a station, it is assumed
 * that the station is licensed with Tridium license features "fileDriver" and
 * "historyArchive". The BFileNetwork must also be installed in the station to import
 * file data to model as local histories. Those local histories can be configured with a
 * limited (small) rolling capacity, leaving older (archived) history data in the source
 * file. This archived file data will then be accessed on-demand via this
 * BFileArchiveHistoryProvider to supplement local history data when history queries
 * occur at runtime.
 *
 * PLEASE NOTE THAT THIS CLASS IS INTENDED TO BE AN EXAMPLE IMPLEMENTATION ONLY!
 * IT IS NOT MEANT FOR PRODUCTION USE SINCE IT IS NOT OPTIMIZED IN TERMS OF MEMORY AND
 * PERFORMANCE, AND IT ALSO RELIES ON SOME NON-PUBLIC NIAGARA APIs.
 *
 * @author Scott Hoye on 02/17/2022
 * @since Niagara 4.11
 */
@NiagaraType
public class BFileArchiveHistoryProvider
  extends BArchiveHistoryProvider
{
//region /*+ ------------ BEGIN BAJA AUTO GENERATED CODE ------------ +*/
//@formatter:off
/*@ $com.tridium.fileArchiveHistory.BFileArchiveHistoryProvider(2979906276)1.0$ @*/
/* Generated Mon Feb 07 16:15:23 EST 2022 by Slot-o-Matic (c) Tridium, Inc. 2012-2022 */

  //region Type

  @Override
  public Type getType() { return TYPE; }
  public static final Type TYPE = Sys.loadType(BFileArchiveHistoryProvider.class);

  //endregion Type

//@formatter:on
//endregion /*+ ------------ END BAJA AUTO GENERATED CODE -------------- +*/

//region BArchiveHistoryProvider overrides

  /**
   * Since multiple {@link BArchiveHistoryProvider} instances can be installed
   * in the HistoryService's {@link BArchiveHistoryProviders} container, at
   * history query time, this callback is given to provide a quick, unreliable
   * existence check for archived history data accessible by this provider
   * instance.  This overridden implementation checks to see if a
   * {@link BDelimitedFileImport} descriptor exists in a {@link BFileNetwork}
   * on the local station that is configured for a History ID that matches the
   * one in the given {@link BHistoryConfig} parameter. It returns true only if
   * a matching descriptor is found because that is a good indication that archived
   * history data likely exists in the file that the descriptor references.
   */
  @Override
  public boolean isLikelyToContainArchivedHistory(BHistoryConfig historyConfig, Context context)
  {
    // Check for a matching history ID on an existing BDelimitedFileImport
    // descriptor under the BFileNetwork and return true if one is found
    return findFileImportForHistory(historyConfig).isPresent();
  }

  /**
   * This callback occurs when a history (identified by the historyConfig parameter)
   * is being queried. This overridden method will check to see if a
   * {@link BDelimitedFileImport} descriptor exists in a {@link BFileNetwork} on the
   * local station that is configured for a History ID that matches the one in the given
   * {@link BHistoryConfig} parameter. If a match is found, it will use that
   * descriptor's configuration to read archived history data from its
   * referenced source file and return a Cursor (wrapped in an {@link Optional})
   * of filtered history records read from the file that meet the criteria
   * provided as parameters to this method. The returned Cursor will also include
   * special {@link Context} facets to indicate to the caller if the limit
   * was exceeded as well as the pre and post history records (the archive history
   * records just outside the boundaries of the requested time range or just outside
   * the boundaries of the limited results, whichever applies).
   * If no matching descriptor is found, {@link Optional#empty()} will be returned,
   * indicating to the caller that this provider does not contain archived history data
   * for the given history.
   */
  @Override
  protected Optional<Cursor<BHistoryRecord>> doTimeQuery(BHistoryConfig historyConfig,
                                                         BAbsTime startTime, BAbsTime endTime,
                                                         boolean descending, int limit,
                                                         Context cx)
  {
    // Find an existing BDelimitedFileImport descriptor with a matching history ID
    Optional<BDelimitedFileImport> fileImportOptional = findFileImportForHistory(historyConfig);
    if (!fileImportOptional.isPresent())
    {
      // No match found
      return Optional.empty();
    }

    BDelimitedFileImport fileImport = fileImportOptional.get();
    try
    {
      // Found a matching descriptor, so read/sort all archived records from the descriptor's file
      List<BHistoryRecord> sortedRecordsFromFile =
        readAndSortAllRecordsFromFile(historyConfig, fileImport, descending);

      // Return a Cursor loaded with the records read from the file that meet the history query
      // parameters (the Cursor's special Context Facets will also be generated for this result).
      FileArchiveRecordCursor resultCursor =
        new FileArchiveRecordCursor(sortedRecordsFromFile, startTime, endTime, descending, limit);

      return Optional.of(resultCursor);
    }
    catch(Exception e)
    {
      throw new BajaRuntimeException("File archive time query failed for " + toPathString(), e);
    }
  }

//endregion BArchiveHistoryProvider overrides

//region FileArchiveRecordCursor inner class

  /**
   * This inner class implements the {@link Cursor} interface and is used as the
   * result of a {@link #doTimeQuery(BHistoryConfig, BAbsTime, BAbsTime, boolean, int, Context)}
   * request.
   */
  private static class FileArchiveRecordCursor
    implements Cursor<BHistoryRecord>
  {
    /**
     * Constructor that passes in the all sorted, unfiltered archived history
     * records (read from a file) along with the history query parameters to
     * use for filtering the records of the result Cursor.
     */
    FileArchiveRecordCursor(List<BHistoryRecord> allSortedRecordsFromFile,
                            BAbsTime startTime, BAbsTime endTime,
                            boolean descending, int limit)
    {
      this.startTime = startTime;
      this.endTime = endTime;
      this.descending = descending;
      this.limit = limit;

      // Load the Cursor results with the records that meet the history query criteria.
      // Since the input records are already sorted based on the requested ascending/descending
      // timestamp order, the loading can stop when the requested time range is passed while
      // iterating through the records (or when the limit is reached in descending-order mode,
      // since most recent records are favored in the result). The Cursor's special Context
      // Facets will also be generated while loading.
      for (BHistoryRecord record : allSortedRecordsFromFile)
      {
        if (!addRecordIfIncluded(record))
        {
          // Loading is complete, so immediately stop iterating
          break;
        }
      }
    }

    /**
     * Convenience method to check whether the given history record should be included
     * in this Cursor's results, and if so, add it to the Cursor's ordered results. This method
     * returns false when the requested time range is passed (based on the timestamp of the
     * given record) or when the limit is reached in descending-order mode, since most
     * recent records are favored in the result.
     */
    private boolean addRecordIfIncluded(BHistoryRecord record)
    {
      BAbsTime recTimestamp = record.getTimestamp();

      // First check if the record's timestamp is before the requested start time or
      // after the requested end time (note that a NULL start time means the beginning
      // of time and a NULL end time means the end of time)
      if (!startTime.isNull() && recTimestamp.isBefore(startTime) ||
          !endTime.isNull() && recTimestamp.isAfter(endTime))
      {
        if (historyRecords.isEmpty())
        {
          // The record must be before the requested time range since we haven't
          // accepted any records in the result yet, so stash it away in the running preRec
          // variable (used in the Cursor's Context) and return true to tell the caller to
          // keep iterating through the records.
          preRec = record;
          return true;
        }
        else
        {
          // The record must be after the requested time range since the records are sorted
          // and we've previously added some results. That means the given record must be
          // the record immediate trailing the requested time range, so stash that away as
          // the postRec variable (used in the Cursor's Context). Then return false to tell
          // the caller that loading is complete.
          postRec = record;
          return false;
        }
      }

      // Getting here means the record is in the requested time range.

      // Before adding it to the result set, we need to check to see if
      // the limit has been exceeded. The behavior is slightly different between
      // the ascending/descending order use cases since the most recent records
      // must be favored in the result
      if (historyRecords.size() == limit)
      {
        // Flag that the limit was exceeded (used in the Cursor's Context)
        limitExceeded = true;

        if (descending)
        {
          // When descending order is requested, this record must be the first valid one
          // after the limit was reached. So stash the record away as the postRec
          // variable (used in the Cursor's Context). When descending order is requested,
          // that means we've already loaded the most recent records, so return false to tell
          // the caller that loading is now complete.
          postRec = record;
          return false;
        }
        else
        {
          // When ascending order is requested and the limit is exceeded, the oldest
          // record in the capped results needs to be removed to make room for the
          // given record since this one must be more recent. The oldest record removed
          // will also be stashed away as the new preRec variable (used in the Cursor's
          // Context). Returning true is also necessary to keep looking for more recent
          // records that are valid for the requested time range.
          preRec = historyRecords.poll();
        }
      }

      // Add the record to the end of the Cursor's result list and return true to tell
      // the caller to keep iterating through the records.
      historyRecords.add(record);
      return true;
    }

    /**
     * Overridden to return the Cursor's Context, which includes the following
     * special Context {@link javax.baja.sys.BFacets} when applicable:
     *  "historyRecordTypeSpec" - indicates the history record type,
     *  "historyCursorPreRec" - the history record just prior to the requested time range
     *    subject to order and limit (if one exists),
     *  "historyCursorPostRec" - the history record just after the requested time range
     *    subject to order and limit (if one exists),
     *  "archiveHistoryLimitExceeded" - indicates whether more archive records match
     *    the query criteria than can fit into the limit.
     */
    @Override
    public Context getContext()
    {
      if (context == null)
      {
        // Lazily load and stash away the Context to return from this method.
        // Only one thread should be processing the Cursor, so concurrency was
        // not considered in this implementation, but even if multiple threads
        // entered this section simultaneously, they should generate the same result.
        try
        {
          // Use convenience method to add the "historyRecordTypeSpec", "historyCursorPreRec",
          // and "historyCursorPostRec" special Context facets if needed
          context = HistoryCursor.makeBoundaryRecordFacets(preRec, postRec);
        }
        catch (IOException e)
        {
          throw new BajaRuntimeException("Unexpected exception while creating Cursor's boundary record Facets", e);
        }

        if (limitExceeded)
        {
          // If the limit was exceeded, use convenience method to add the
          // "archiveHistoryLimitExceeded" special Context facet
          context = HistoryCursor.makeArchiveLimitExceededContext(context);
        }
      }

      return context;
    }

    /**
     * The Cursor is initially placed before the first history record in
     * the result. This method must be called first, before calling {@link #get()},
     * and can then be called iteratively. Calling this method advances the
     * Cursor to the next record, and returns true if it is positioned on a
     * valid record, or false if the Cursor has reached the end of the iteration.
     */
    @Override
    public boolean next()
    {
      currentRecord = historyRecords.poll();
      return currentRecord != null;
    }

    /**
     * Get the history record at the current Cursor position.
     */
    @Override
    public BHistoryRecord get()
    {
      return currentRecord;
    }

    /**
     * Overridden to satisfy Cursor's implementation of {@link AutoCloseable}.
     * While not applicable here, note that {@link Cursor#close()} says that
     * this method should never throw a checked exception.
     */
    @Override
    public void close()
    {
      historyRecords.clear();
      currentRecord = null;
      preRec = null;
      postRec = null;
    }

    private final BAbsTime startTime;
    private final BAbsTime endTime;
    private final boolean descending;
    private final int limit;

    // This Cursor is backed by a LinkedList. This is not ideal since a Cursor should
    // really optimize memory by streaming results. Refer to the note in the class header.
    private final LinkedList<BHistoryRecord> historyRecords = new LinkedList<>();

    private BHistoryRecord currentRecord;
    private Context context;
    private BHistoryRecord preRec;
    private BHistoryRecord postRec;
    private boolean limitExceeded;
  }

//endregion FileArchiveRecordCursor inner class

}
