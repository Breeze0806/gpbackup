package restore

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/pkg/errors"
)

/*
 * Functions to run commands on entire cluster during restore
 */

func VerifyBackupDirectoriesExistOnAllHosts() {
	_, err := globalCluster.ExecuteLocalCommand(fmt.Sprintf("test -d %s", globalFPInfo.GetDirForContent(-1)))
	gplog.FatalOnError(err, "Backup directory %s missing or inaccessible", globalFPInfo.GetDirForContent(-1))
	if MustGetFlagString(options.PLUGIN_CONFIG) == "" || backupConfig.SingleDataFile {
		contentMap := GetResizeContentMap()
		remoteOutput := globalCluster.GenerateAndExecuteCommand("Verifying backup directories exist", cluster.ON_SEGMENTS, func(contentID int) string {
			destContent, exists := contentMap[contentID]
			if exists {
				return fmt.Sprintf("test -d %s", globalFPInfo.GetDirForContent(destContent))
			} else { // Don't check for directories for contents that aren't part of the backup set
				return ""
			}
		})
		globalCluster.CheckClusterError(remoteOutput, "Backup directories missing or inaccessible", func(contentID int) string {
			return fmt.Sprintf("Backup directory %s missing or inaccessible", globalFPInfo.GetDirForContent(contentID))
		})
	}
}

func VerifyBackupFileCountOnSegments(fileCount int) {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Verifying backup file count", cluster.ON_SEGMENTS, func(contentID int) string {
		return fmt.Sprintf("find %s -type f | wc -l", globalFPInfo.GetDirForContent(contentID))
	})
	globalCluster.CheckClusterError(remoteOutput, "Could not verify backup file count", func(contentID int) string {
		return "Could not verify backup file count"
	})

	numIncorrect := 0
	for contentID, cmd := range remoteOutput.Commands {
		numFound, _ := strconv.Atoi(strings.TrimSpace(cmd.Stdout))
		if numFound != fileCount {
			gplog.Verbose("Expected to find %d file(s) on segment %d on host %s, but found %d instead.", fileCount, contentID, globalCluster.GetHostForContent(contentID), numFound)
			numIncorrect++
		}
	}
	if numIncorrect > 0 {
		cluster.LogFatalClusterError("Found incorrect number of backup files", cluster.ON_SEGMENTS, numIncorrect)
	}
}

func VerifyMetadataFilePaths(withStats bool) {
	filetypes := []string{"config", "table of contents", "metadata"}
	missing := false
	for _, filetype := range filetypes {
		filepath := globalFPInfo.GetBackupFilePath(filetype)
		if !iohelper.FileExistsAndIsReadable(filepath) {
			missing = true
			gplog.Error("Cannot access %s file %s", filetype, filepath)
		}
	}
	if withStats {
		filepath := globalFPInfo.GetStatisticsFilePath()
		if !iohelper.FileExistsAndIsReadable(filepath) {
			missing = true
			gplog.Error("Cannot access statistics file %s", filepath)
			gplog.Error(`Note that the "-with-stats" flag must be passed to gpbackup to generate a statistics file.`)
		}
	}
	if missing {
		gplog.Fatal(errors.Errorf("One or more metadata files do not exist or are not readable."), "Cannot proceed with restore")
	}
}
