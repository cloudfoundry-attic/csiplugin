package csiplugin_test

import (
	"os"
	"time"

	"code.cloudfoundry.org/goshims/grpcshim/grpc_fake"
	"code.cloudfoundry.org/goshims/osshim/os_fake"
	"code.cloudfoundry.org/lager/lagertest"
	"code.cloudfoundry.org/volman"
	"github.com/Kaixiang/csiplugin"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/paulcwarren/spec"
	"github.com/paulcwarren/spec/csishim/csi_fake"
)

var _ = Describe("CsiPluginNode", func() {

	var (
		fakePluginSpec volman.PluginSpec
		fakeNodeClient *csi_fake.FakeNodeClient
		csiPlugin      volman.Plugin
		logger         *lagertest.TestLogger
		err            error
		fakeGrpc       *grpc_fake.FakeGrpc
		conn           *grpc_fake.FakeClientConn
		fakeCsi        *csi_fake.FakeCsi
		fakeOs         *os_fake.FakeOs
		mountResponse  volman.MountResponse
		fileInfo       *FakeFileInfo
		volumesRootDir string
	)

	BeforeEach(func() {
		fakePluginSpec = volman.PluginSpec{
			Name:      "fakecsi",
			Address:   "127.0.0.1:1234",
			TLSConfig: &volman.TLSConfig{},
		}
		fakeNodeClient = &csi_fake.FakeNodeClient{}
		logger = lagertest.NewTestLogger("csi-plugin-node-test")
		fakeGrpc = &grpc_fake.FakeGrpc{}
		fakeCsi = &csi_fake.FakeCsi{}
		fakeOs = &os_fake.FakeOs{}
		fakeCsi.NewNodeClientReturns(fakeNodeClient)
		volumesRootDir = "/var/vcap/data/mount"
		csiPlugin = csiplugin.NewCsiPlugin(fakeNodeClient, fakePluginSpec, fakeGrpc, fakeCsi, fakeOs, volumesRootDir)
		conn = new(grpc_fake.FakeClientConn)
		fakeGrpc.DialReturns(conn, nil)
	})

	JustBeforeEach(func() {
		mountResponse, err = csiPlugin.Mount(logger, "fakedriverid", "fakevolumeid", map[string]interface{}{})
	})

	Describe("#Mount", func() {
		BeforeEach(func() {
			fakeNodeClient.NodePublishVolumeReturns(&csi.NodePublishVolumeResponse{
				Reply: &csi.NodePublishVolumeResponse_Result_{
					Result: &csi.NodePublishVolumeResponse_Result{},
				},
			}, nil)
		})

		Context("when the volumesRoot doen't exist", func() {
			BeforeEach(func() {
				fileInfo = newFakeFileInfo()
				err = os.ErrNotExist
				fakeOs.StatReturns(fileInfo, err)
				fakeOs.IsNotExistReturns(true)
			})

			It("Create volumesRoot directory and Send publish request to CSI node server", func() {
				Expect(err).ToNot(HaveOccurred())
				expectedResponse := &volman.MountResponse{
					Path: volumesRootDir + "/fakevolumeid",
				}
				Expect(fakeOs.MkdirAllCallCount()).To(Equal(1))
				Expect(fakeNodeClient.NodePublishVolumeCallCount()).To(Equal(1))
				Expect(mountResponse.Path).To(Equal(expectedResponse.Path))
				Expect(conn.CloseCallCount()).To(Equal(1))
			})
		})

		Context("when the volumesRoot exist", func() {
			BeforeEach(func() {
				fileInfo = newFakeFileInfo()
				err = os.ErrNotExist
				fakeOs.StatReturns(fileInfo, err)
				fakeOs.IsNotExistReturns(false)
			})

			It("Keeps going with existing volumesRoot directory", func() {
				Expect(err).ToNot(HaveOccurred())
				expectedResponse := &volman.MountResponse{
					Path: volumesRootDir + "/fakevolumeid",
				}
				Expect(fakeOs.MkdirAllCallCount()).To(Equal(0))
				Expect(fakeNodeClient.NodePublishVolumeCallCount()).To(Equal(1))
				Expect(mountResponse.Path).To(Equal(expectedResponse.Path))
				Expect(conn.CloseCallCount()).To(Equal(1))
			})
		})

		Context("When csi node server response some error", func() {
			BeforeEach(func() {
				fakeNodeClient.NodePublishVolumeReturns(&csi.NodePublishVolumeResponse{
					Reply: &csi.NodePublishVolumeResponse_Error{
						Error: &csi.Error{
							Value: &csi.Error_NodePublishVolumeError_{
								NodePublishVolumeError: &csi.Error_NodePublishVolumeError{
									ErrorCode:        csi.Error_NodePublishVolumeError_MOUNT_ERROR,
									ErrorDescription: "Error mounting volume",
								},
							},
						},
					},
				}, nil)
			})

			It("report error and log it", func() {
				Expect(err).To(HaveOccurred())
				expectedResponse := volman.MountResponse{}
				Expect(fakeNodeClient.NodePublishVolumeCallCount()).To(Equal(1))
				Expect(mountResponse).To(Equal(expectedResponse))
				Expect(logger.Buffer()).To(gbytes.Say("Error mounting volume"))
				Expect(conn.CloseCallCount()).To(Equal(1))
			})
		})
	})
})

type FakeFileInfo struct {
	FileMode os.FileMode
}

func (FakeFileInfo) Name() string                { return "" }
func (FakeFileInfo) Size() int64                 { return 0 }
func (fs *FakeFileInfo) Mode() os.FileMode       { return fs.FileMode }
func (fs *FakeFileInfo) StubMode(fm os.FileMode) { fs.FileMode = fm }
func (FakeFileInfo) ModTime() time.Time          { return time.Time{} }
func (FakeFileInfo) IsDir() bool                 { return false }
func (FakeFileInfo) Sys() interface{}            { return nil }

func newFakeFileInfo() *FakeFileInfo {
	return &FakeFileInfo{}
}
