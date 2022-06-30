package restore

/*
 * This file contains structs and functions related to backing up data on the segments.
 */

import (
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/jackc/pgconn"
	"github.com/pkg/errors"
	"gopkg.in/cheggaaa/pb.v1"
)

var (
	tableDelim       = ","
	resizeBatchMap   map[int]map[int]int
	resizeContentMap map[int]int
)

func CopyTableIn(connectionPool *dbconn.DBConn, tableName string, tableAttributes string, destinationToRead string, singleDataFile bool, whichConn int) (int64, error) {
	whichConn = connectionPool.ValidateConnNum(whichConn)
	copyCommand := ""
	readFromDestinationCommand := "cat"
	customPipeThroughCommand := utils.GetPipeThroughProgram().InputCommand

	if singleDataFile {
		//helper.go handles compression, so we don't want to set it here
		customPipeThroughCommand = "cat -"
	} else if MustGetFlagString(options.PLUGIN_CONFIG) != "" {
		readFromDestinationCommand = fmt.Sprintf("%s restore_data %s", pluginConfig.ExecutablePath, pluginConfig.ConfigPath)
	}

	copyCommand = fmt.Sprintf("PROGRAM '%s %s | %s'", readFromDestinationCommand, destinationToRead, customPipeThroughCommand)

	query := fmt.Sprintf("COPY %s%s FROM %s WITH CSV DELIMITER '%s'", tableName, tableAttributes, copyCommand, tableDelim)
	onSegmentQuery := query + " ON SEGMENT"

	var numRows, rowsLoaded int64
	var err error

	// We generate the resize mappings in all cases, because a "normal" restore is identical to restoring a single batch
	// of N origin segments to N destination segments, so the normal and resize restores can use the same code path in
	// cases where a batch has one content for each destination segment.
	batchMap := GetResizeBatchMap()
	destSize := len(globalCluster.ContentIDs) - 1

	for _, contentMap := range batchMap {
		if len(contentMap) == destSize {
			// Where segment counts match, we can use COPY ON SEGMENT to load data with database parallelism.
			rowsLoaded, err = loadTableDataUsingCopyOnSegment(onSegmentQuery, tableName, whichConn)

		} else {
			// Where segment counts don't match, we'll need to load data onto segments individually.
			rowsLoaded, err = loadTableDataUsingParallelCopy(query, tableName, contentMap)
		}
		if err != nil {
			break
		}
		numRows += rowsLoaded
	}

	return numRows, err
}

// The content mapping won't change during a given restore, so only generate it
// the first time and return the generated maps on all subsequent calls.  The
// GenerateResizeMaps function is separated out for testing purposes.
//
// The resizeBatchMap is used for the actual data restore, since that cares about
// the batch in which a given content is restored, while the resizeContentMap is
// used for file checks because that only cares about file locations.

func GetResizeBatchMap() map[int]map[int]int {
	if resizeBatchMap == nil {
		destSize := len(globalCluster.ContentIDs) - 1
		origSize := backupConfig.SegmentCount
		if origSize == 0 {
			origSize = destSize
		}
		resizeBatchMap, resizeContentMap = GenerateResizeMaps(origSize, destSize)
	}
	return resizeBatchMap
}

func GetResizeContentMap() map[int]int {
	if resizeContentMap == nil {
		destSize := len(globalCluster.ContentIDs) - 1
		origSize := backupConfig.SegmentCount
		if origSize == 0 {
			origSize = destSize
		}
		resizeBatchMap, resizeContentMap = GenerateResizeMaps(origSize, destSize)
	}
	return resizeContentMap
}

// Returns a map of batches of origin-to-destination pairs.  Each pair maps one content from
// the origin (backup) cluster to the content in the destination (restore) cluster to which
// that segment's data will be restored.  Each batch contains a number of pairs less than or
// equal to the size of the restore cluster.
//
// For example, if the backup cluster had 5 segments and the restore cluster has 3 segments,
// the first batch will contain the pairs (0,0), (1,1), and (2,2) because the first three
// backup segments map to the same restore segments, and the second batch will contain the
// pairs (4,0) and (5,1) because the next two backup segments map to the first two restore
// segments.
func GenerateResizeMaps(origSize int, destSize int) (map[int]map[int]int, map[int]int) {
	batchMap := make(map[int]map[int]int)
	contentMap := make(map[int]int)
	batch := make(map[int]int)
	if origSize <= destSize {
		for i := 0; i < origSize; i++ {
			batch[i] = i
		}
		batchMap[0] = batch
		contentMap = batch
	} else if origSize > destSize {
		batchNum := 0
		currContent := 0
		batchMap[0] = make(map[int]int)
		for origContent := 0; origContent < origSize; origContent++ {
			destContent := origContent % destSize
			if currContent == destSize {
				batchNum++
				batchMap[batchNum] = make(map[int]int)
				currContent = 0
			}
			batchMap[batchNum][origContent] = destContent
			contentMap[origContent] = destContent
			currContent++
		}
	}
	return batchMap, contentMap
}

// Load backup data with COPY ON SEGMENT.  Used when restoring to a cluster with
// the same number of segments as the backup segment, whether performing a "normal"
// restore or a "resize" restore.
func loadTableDataUsingCopyOnSegment(query string, tableName string, whichConn int) (int64, error) {
	gplog.Verbose(`Executing "%s" on master`, query)
	result, err := connectionPool.Exec(query, whichConn)
	if err != nil {
		errStr := fmt.Sprintf("Error loading data into table %s", tableName)

		// The COPY ON SEGMENT error might contain useful CONTEXT output
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Where != "" {
			errStr = fmt.Sprintf("%s: %s", errStr, pgErr.Where)
		}

		return 0, errors.Wrap(err, errStr)
	}
	numRows, _ := result.RowsAffected()
	return numRows, nil
}

// Load backup data to individual segments using one goroutine per segment.  Used
// when restoring to a cluster that is of a different size than the cluster on
// which a backup was taken.
func loadTableDataUsingParallelCopy(query string, tableName string, contentMap map[int]int) (int64, error) {
	var workerPool sync.WaitGroup
	var copyErrs chan error
	var numRows int64

	dbName := utils.UnquoteIdent(backupConfig.DatabaseName)
	if MustGetFlagString(options.REDIRECT_DB) != "" {
		dbName = MustGetFlagString(options.REDIRECT_DB)
	}

	segidRegex := regexp.MustCompile(`<SEGID>`)
	dirRegex := regexp.MustCompile(`<SEG_DATA_DIR>`)
	for origContent, destContent := range contentMap {
		workerPool.Add(1)
		go func(backupContent int, restoreContent int) {
			defer workerPool.Done()

			segmentConn := setupSegmentUtilityConnection(dbName, restoreContent)
			defer segmentConn.Close()

			copyQuery := segidRegex.ReplaceAllString(query, strconv.Itoa(backupContent))
			copyQuery = dirRegex.ReplaceAllString(copyQuery, globalCluster.GetDirForContent(backupContent))
			gplog.Verbose(`Executing "%s" on segment %d`, copyQuery, restoreContent)
			gplog.Verbose("Restoring data for %s from backup segment %d into restore segment %d", tableName, backupContent, restoreContent)

			result, err := segmentConn.Exec(copyQuery)

			if err != nil {
				errStr := fmt.Sprintf("Error loading data into table %s on segment %d", tableName, restoreContent)

				// The COPY error might contain useful CONTEXT output
				if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Where != "" {
					errStr = fmt.Sprintf("%s: %s", errStr, pgErr.Where)
				}

				copyErrs <- errors.Wrap(err, errStr)
			} else {
				rowsLoaded, _ := result.RowsAffected()
				atomic.AddInt64(&numRows, rowsLoaded)
			}
		}(origContent, destContent)
	}
	workerPool.Wait()

	if len(copyErrs) != 0 {
		return 0, <-copyErrs // We only return one error, so we don't care which segment's error this is
	}

	return numRows, nil
}

func setupSegmentUtilityConnection(dbname string, content int) *dbconn.DBConn {
	username := operating.System.Getenv("PGUSER")
	if username == "" {
		currentUser, _ := operating.System.CurrentUser()
		username = currentUser.Username
	}
	host := globalCluster.GetHostForContent(content)
	port := globalCluster.GetPortForContent(content)
	conn := dbconn.NewDBConn(dbname, username, host, port)
	conn.MustConnectInUtilityMode(1)
	conn.MustExec("SET log_statement = 'all'")
	return conn
}

func restoreSingleTableData(fpInfo *filepath.FilePathInfo, entry toc.MasterDataEntry, tableName string, whichConn int) error {
	destinationToRead := ""
	if backupConfig.SingleDataFile {
		destinationToRead = fmt.Sprintf("%s_%d", fpInfo.GetSegmentPipePathForCopyCommand(), entry.Oid)
	} else {
		destinationToRead = fpInfo.GetTableBackupFilePathForCopyCommand(entry.Oid, utils.GetPipeThroughProgram().Extension, backupConfig.SingleDataFile)
	}
	numRowsRestored, err := CopyTableIn(connectionPool, tableName, entry.AttributeString, destinationToRead, backupConfig.SingleDataFile, whichConn)
	if err != nil {
		return err
	}
	numRowsBackedUp := entry.RowsCopied
	err = CheckRowsRestored(numRowsRestored, numRowsBackedUp, tableName)
	if err != nil {
		return err
	}
	resizeCluster := MustGetFlagBool(options.RESIZE_CLUSTER)
	if resizeCluster {
		gplog.Debug("Redistributing data for %s", tableName)
		err = RedistributeTableData(tableName, whichConn)
	}
	return nil
}

func CheckRowsRestored(rowsRestored int64, rowsBackedUp int64, tableName string) error {
	if rowsRestored != rowsBackedUp {
		rowsErrMsg := fmt.Sprintf("Expected to restore %d rows to table %s, but restored %d instead", rowsBackedUp, tableName, rowsRestored)
		return errors.New(rowsErrMsg)
	}
	return nil
}

func RedistributeTableData(tableName string, whichConn int) error {
	query := fmt.Sprintf("ALTER TABLE %s SET WITH (REORGANIZE=true)", tableName)
	_, err := connectionPool.Exec(query, whichConn)
	return err
}

func restoreDataFromTimestamp(fpInfo filepath.FilePathInfo, dataEntries []toc.MasterDataEntry,
	gucStatements []toc.StatementWithType, dataProgressBar utils.ProgressBar) int32 {
	totalTables := len(dataEntries)
	if totalTables == 0 {
		gplog.Verbose("No data to restore for timestamp = %s", fpInfo.Timestamp)
		return 0
	}

	if backupConfig.SingleDataFile {
		gplog.Verbose("Initializing pipes and gpbackup_helper on segments for single data file restore")
		utils.VerifyHelperVersionOnSegments(version, globalCluster)
		oidList := make([]string, totalTables)
		for i, entry := range dataEntries {
			oidList[i] = fmt.Sprintf("%d", entry.Oid)
		}
		utils.WriteOidListToSegments(oidList, globalCluster, fpInfo)
		initialPipes := CreateInitialSegmentPipes(oidList, globalCluster, connectionPool, fpInfo)
		if wasTerminated {
			return 0
		}
		isFilter := false
		if len(opts.IncludedRelations) > 0 || len(opts.ExcludedRelations) > 0 || len(opts.IncludedSchemas) > 0 || len(opts.ExcludedSchemas) > 0 {
			isFilter = true
		}
		utils.StartGpbackupHelpers(globalCluster, fpInfo, "--restore-agent", MustGetFlagString(options.PLUGIN_CONFIG), "", MustGetFlagBool(options.ON_ERROR_CONTINUE), isFilter, &wasTerminated, initialPipes, resizeContentMap)
	}
	/*
	 * We break when an interrupt is received and rely on
	 * TerminateHangingCopySessions to kill any COPY
	 * statements in progress if they don't finish on their own.
	 */
	var tableNum int64 = 0
	tasks := make(chan toc.MasterDataEntry, totalTables)
	var workerPool sync.WaitGroup
	var numErrors int32
	var mutex = &sync.Mutex{}

	for i := 0; i < connectionPool.NumConns; i++ {
		workerPool.Add(1)
		go func(whichConn int) {
			defer workerPool.Done()

			setGUCsForConnection(gucStatements, whichConn)
			for entry := range tasks {
				if wasTerminated {
					dataProgressBar.(*pb.ProgressBar).NotPrint = true
					return
				}
				tableName := utils.MakeFQN(entry.Schema, entry.Name)
				if opts.RedirectSchema != "" {
					tableName = utils.MakeFQN(opts.RedirectSchema, entry.Name)
				}
				// Truncate table before restore, if needed
				var err error
				if MustGetFlagBool(options.INCREMENTAL) || MustGetFlagBool(options.TRUNCATE_TABLE) {
					err = TruncateTable(tableName, whichConn)
				}
				if err == nil {
					err = restoreSingleTableData(&fpInfo, entry, tableName, whichConn)

					if gplog.GetVerbosity() > gplog.LOGINFO {
						// No progress bar at this log level, so we note table count here
						gplog.Verbose("Restored data to table %s from file (table %d of %d)", tableName, atomic.AddInt64(&tableNum, 1), totalTables)
					} else {
						gplog.Verbose("Restored data to table %s from file", tableName)
					}
				}

				if err != nil {
					gplog.Error(err.Error())
					atomic.AddInt32(&numErrors, 1)
					if !MustGetFlagBool(options.ON_ERROR_CONTINUE) {
						dataProgressBar.(*pb.ProgressBar).NotPrint = true
						return
					} else if connectionPool.Version.AtLeast("6") && backupConfig.SingleDataFile {
						// inform segment helpers to skip this entry
						utils.CreateSkipFileOnSegments(fmt.Sprintf("%d", entry.Oid), tableName, globalCluster, globalFPInfo)
					}
					mutex.Lock()
					errorTablesData[tableName] = Empty{}
					mutex.Unlock()
				}

				if backupConfig.SingleDataFile {
					agentErr := utils.CheckAgentErrorsOnSegments(globalCluster, globalFPInfo)
					if agentErr != nil {
						gplog.Error(agentErr.Error())
						return
					}
				}

				dataProgressBar.Increment()
			}
		}(i)
	}
	for _, entry := range dataEntries {
		tasks <- entry
	}
	close(tasks)
	workerPool.Wait()

	if numErrors > 0 {
		fmt.Println("")
		gplog.Error("Encountered %d error(s) during table data restore; see log file %s for a list of table errors.", numErrors, gplog.GetLogFilePath())
	}

	return numErrors
}

func CreateInitialSegmentPipes(oidList []string, c *cluster.Cluster, connectionPool *dbconn.DBConn, fpInfo filepath.FilePathInfo) int {
	// Create min(connections, tables) segment pipes on each host
	var maxPipes int
	if connectionPool.NumConns < len(oidList) {
		maxPipes = connectionPool.NumConns
	} else {
		maxPipes = len(oidList)
	}
	for i := 0; i < maxPipes; i++ {
		utils.CreateSegmentPipeOnAllHosts(oidList[i], c, fpInfo)
	}
	return maxPipes
}
