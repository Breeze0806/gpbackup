package backup_test

import (
	"path/filepath"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	"github.com/blang/vfs"
	"github.com/blang/vfs/memfs"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/pkg/errors"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("gpexpand_sensor", func() {
	const sampleMasterDataDir = "/my_fake_database/demoDataDir-1"
	var (
		memoryfs   vfs.Filesystem
		mddPathRow *sqlmock.Rows
	)

	BeforeEach(func() {
		memoryfs = memfs.Create()
		mddPathRow = sqlmock.NewRows([]string{"datadir"}).AddRow(sampleMasterDataDir)
		// simulate that database connection is to postgres database, with Greenplum 6+
		connectionPool.DBName = "postgres"
		testhelper.SetDBVersion(connectionPool, "6.0.0")
	})
	Context("IsGpexpandRunning", func() {
		Describe("happy path", func() {
			It("senses when gpexpand is in phase 1, as determined by existence of a file 'gpexpand.status' in master data directory", func() {
				mock.ExpectQuery(backup.MasterDataDirQuery).WillReturnRows(mddPathRow)
				Expect(vfs.MkdirAll(memoryfs, sampleMasterDataDir, 755)).To(Succeed())
				path := filepath.Join(sampleMasterDataDir, backup.GpexpandStatusFilename)
				Expect(vfs.WriteFile(memoryfs, path, []byte{0}, 0400)).To(Succeed())
				gpexpandSensor := backup.NewGpexpandSensor(memoryfs, connectionPool)

				result, err := gpexpandSensor.IsGpexpandRunning()

				Expect(err).ToNot(HaveOccurred())
				Expect(result).To(BeTrue())
			})
			It("senses gpexpand is in phase 2, as determined by database query to postgres database for gpexpand's temporary table", func() {
				mock.ExpectQuery(backup.MasterDataDirQuery).WillReturnRows(mddPathRow)
				hasGpexpandPhase2StatusRow := sqlmock.NewRows([]string{"status"}).AddRow("some gpexpand status")
				mock.ExpectQuery(backup.GpexpandTemporaryTableStatusQuery).WillReturnRows(hasGpexpandPhase2StatusRow)
				gpexpandSensor := backup.NewGpexpandSensor(memoryfs, connectionPool)

				result, err := gpexpandSensor.IsGpexpandRunning()

				Expect(err).ToNot(HaveOccurred())
				Expect(result).To(BeTrue())
			})
			It("senses when all indications are that gpexpand is not running", func() {
				mock.ExpectQuery(backup.MasterDataDirQuery).WillReturnRows(mddPathRow)
				mock.ExpectQuery(backup.GpexpandTemporaryTableStatusQuery).WillReturnError(errors.New("gpexpand cannot be queried error"))
				gpexpandSensor := backup.NewGpexpandSensor(memoryfs, connectionPool)

				result, err := gpexpandSensor.IsGpexpandRunning()

				Expect(err).ToNot(HaveOccurred())
				Expect(result).To(BeFalse())
			})
		})
		Describe("sad paths", func() {
			It("returns an error when MDD query fails", func() {
				mock.ExpectQuery(backup.MasterDataDirQuery).WillReturnError(errors.New("query error"))
				gpexpandSensor := backup.NewGpexpandSensor(memoryfs, connectionPool)

				_, err := gpexpandSensor.IsGpexpandRunning()

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("query error"))
			})
			It("returns an error when Stat for file fails for a reason besides 'does not exist'", func() {
				mock.ExpectQuery(backup.MasterDataDirQuery).WillReturnRows(mddPathRow)
				gpexpandSensor := backup.NewGpexpandSensor(vfs.Dummy(errors.New("fs error")), connectionPool)

				_, err := gpexpandSensor.IsGpexpandRunning()

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("fs error"))
			})
			It("returns an error when supplied with a connection to a database != postgres", func() {
				connectionPool.DBName = "notThePostgresDatabase"
				gpexpandSensor := backup.NewGpexpandSensor(memoryfs, connectionPool)

				_, err := gpexpandSensor.IsGpexpandRunning()

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("gpexpand sensor requires a connection to the postgres database"))
			})
			It("returns an error when supplied with Greenplum version < 6", func() {
				testhelper.SetDBVersion(connectionPool, "5.3.0")
				gpexpandSensor := backup.NewGpexpandSensor(memoryfs, connectionPool)

				_, err := gpexpandSensor.IsGpexpandRunning()

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("gpexpand sensor requires a connection to Greenplum version >= 6"))
			})
		})
	})
})
