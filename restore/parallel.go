package restore

/*
 * This file contains functions related to executing multiple SQL statements in parallel.
 */

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
)

var (
	mutex = &sync.Mutex{}
)

func executeStatementsForConn(statements chan toc.StatementWithType, fatalErr *error, numErrors *int32, progressBar utils.ProgressBar, whichConn int, executeInParallel bool) {
	for statement := range statements {
		if wasTerminated || *fatalErr != nil {
			return
		}
		_, err := connectionPool.Exec(statement.Statement, whichConn)
		if err != nil {
			gplog.Verbose("Error encountered when executing statement: %s Error was: %s", strings.TrimSpace(statement.Statement), err.Error())
			if MustGetFlagBool(options.ON_ERROR_CONTINUE) {
				if executeInParallel {
					atomic.AddInt32(numErrors, 1)
					mutex.Lock()
					errorTablesMetadata[statement.Schema+"."+statement.Name] = Empty{}
					mutex.Unlock()
				} else {
					*numErrors = *numErrors + 1
					errorTablesMetadata[statement.Schema+"."+statement.Name] = Empty{}
				}
			} else {
				*fatalErr = err
			}
		}
		progressBar.Increment()
	}
}

func ExecutePreDataStatements(statements []toc.StatementWithType, progressBar utils.ProgressBar, retries int) int32 {
	var workerPool sync.WaitGroup
	var fatalErr error
	var numErrors int32 = 0
	numRetries := retries
	deferredStatements := []toc.StatementWithType{}
	splitStatements := make(map[int][]toc.StatementWithType, connectionPool.NumConns)
	currentWorker := 0
	splitStatements[0] = append(splitStatements[0], statements[0])
	for i := 0; i < len(statements) - 1; i++ {
	  if statements[i].TypeIsEqual(statements[i+1]) {
			splitStatements[currentWorker] = append(splitStatements[currentWorker], statements[i+1])
		}	else {
			currentWorker++
			if currentWorker == connectionPool.NumConns {
				currentWorker = 0
			}
			splitStatements[currentWorker] = append(splitStatements[currentWorker], statements[i+1])
		}
	}
	chanMap := make(map[int] chan toc.StatementWithType, connectionPool.NumConns)
	for i := 0; i < connectionPool.NumConns; i++ {
		chanMap[i] = make(chan toc.StatementWithType,len(splitStatements[i]) )
		for _, statement := range splitStatements[i] {
			chanMap[i] <- statement
		}		
	}
	for worker, statement := range chanMap {
		workerPool.Add(1)
		go func(connNum int, statements chan toc.StatementWithType) {
			defer workerPool.Done()
			connNum = connectionPool.ValidateConnNum(connNum)
			for statement := range statements {
				if wasTerminated || fatalErr != nil {
					return
				}
				_, err := connectionPool.Exec(statement.Statement, connNum)
				if err != nil {
					if numRetries == 0 {
						gplog.Verbose("Error encountered when executing statement: %s Error was: %s", statement.Schema+"."+statement.Name, err.Error())
						mutex.Lock()
						errorTablesMetadata[statement.Schema+"."+statement.Name] = Empty{}
						mutex.Unlock()
					} else {
						deferredStatements = append(deferredStatements, statement)
						atomic.AddInt32(&numErrors, 1)
					}
			}
			if fatalErr != nil {
				fmt.Println("")
				gplog.Fatal(fatalErr, "")
			} else {
				progressBar.Increment()
			}
			
			}
			
		}(worker, statement)
		}
	for _, c := range chanMap {
		close(c)
	}
	workerPool.Wait()
	if numErrors > 0 {
		if numRetries > 0 {
			numRetries--
			ExecutePreDataStatements(deferredStatements, progressBar, numRetries)
		} else {
			gplog.Error("Encountered %d errors during metadata restore; see log file %s for a list of failed statements.", numErrors, gplog.GetLogFilePath())
		}		
	}
	return numErrors
}

/*
 * This function creates a worker pool of N goroutines to be able to execute up
 * to N statements in parallel.
 */
func ExecuteStatements(statements []toc.StatementWithType, progressBar utils.ProgressBar, executeInParallel bool, whichConn ...int) int32 {
	var workerPool sync.WaitGroup
	var fatalErr error
	var numErrors int32
	tasks := make(chan toc.StatementWithType, len(statements))
	for _, statement := range statements {
		tasks <- statement
	}
	close(tasks)

	if !executeInParallel {
		connNum := connectionPool.ValidateConnNum(whichConn...)
		executeStatementsForConn(tasks, &fatalErr, &numErrors, progressBar, connNum, executeInParallel)
	} else {
		for i := 0; i < connectionPool.NumConns; i++ {
			workerPool.Add(1)
			go func(connNum int) {
				defer workerPool.Done()
				connNum = connectionPool.ValidateConnNum(connNum)
				executeStatementsForConn(tasks, &fatalErr, &numErrors, progressBar, connNum, executeInParallel)
			}(i)
		}
		workerPool.Wait()
	}
	if fatalErr != nil {
		fmt.Println("")
		gplog.Fatal(fatalErr, "")
	} else if numErrors > 0 {
		fmt.Println("")
		gplog.Error("Encountered %d errors during metadata restore; see log file %s for a list of failed statements.", numErrors, gplog.GetLogFilePath())
	}

	return numErrors
}

func ExecuteStatementsAndCreateProgressBar(statements []toc.StatementWithType, objectsTitle string, showProgressBar int, executeInParallel bool, whichConn ...int) int32 {
	progressBar := utils.NewProgressBar(len(statements), fmt.Sprintf("%s restored: ", objectsTitle), showProgressBar)
	progressBar.Start()
	numErrors := ExecuteStatements(statements, progressBar, executeInParallel, whichConn...)
	progressBar.Finish()

	return numErrors
}

/*
 *   There is an existing bug in Greenplum where creating indexes in parallel
 *   on an AO table that didn't have any indexes previously can cause
 *   deadlock.
 *
 *   We work around this issue by restoring post data objects in
 *   two batches. The first batch takes one index from each table and
 *   restores them in parallel (which has no possibility of deadlock) and
 *   then the second restores all other postdata objects in parallel. After
 *   each table has at least one index, there is no more risk of deadlock.
 *
 *   A third batch is created specifically for postdata metadata
 *   (e.g. ALTER INDEX, ALTER EVENT TRIGGER, COMMENT ON). These
 *   statements cannot be concurrently run with batch two since that
 *   is where the dependent postdata objects are being created.
 */
func BatchPostdataStatements(statements []toc.StatementWithType) ([]toc.StatementWithType, []toc.StatementWithType, []toc.StatementWithType) {
	indexMap := make(map[string]bool)
	firstBatch := make([]toc.StatementWithType, 0)
	secondBatch := make([]toc.StatementWithType, 0)
	thirdBatch := make([]toc.StatementWithType, 0)
	for _, statement := range statements {
		_, tableIndexPresent := indexMap[statement.ReferenceObject]
		if statement.ObjectType == "INDEX" && !tableIndexPresent {
			indexMap[statement.ReferenceObject] = true
			firstBatch = append(firstBatch, statement)
		} else if strings.Contains(statement.ObjectType, " METADATA") {
			thirdBatch = append(thirdBatch, statement)
		} else {
			secondBatch = append(secondBatch, statement)
		}
	}
	return firstBatch, secondBatch, thirdBatch
}
