package csiplugin_test

import (
	"math/rand"
	"sync"
	"time"

	"code.cloudfoundry.org/goshims/grpcshim/grpc_fake"
	"code.cloudfoundry.org/goshims/osshim/os_fake"
	"code.cloudfoundry.org/lager/lagertest"
	"code.cloudfoundry.org/volman"
	"code.cloudfoundry.org/csishim/csi_fake"
	"github.com/Kaixiang/csiplugin"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/container-storage-interface/spec/lib/go/csi"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

	Describe("#Mount", func() {
		JustBeforeEach(func() {
			mountResponse, err = csiPlugin.Mount(logger, "fakevolumeid", map[string]interface{}{})
		})

		BeforeEach(func() {
			fakeNodeClient.NodePublishVolumeReturns(&csi.NodePublishVolumeResponse{}, nil)
		})

		It("should mount the right volume", func() {
			_, request, _ := fakeNodeClient.NodePublishVolumeArgsForCall(0)
			Expect(request.GetVolumeId()).To(Equal("fakevolumeid"))
			Expect(request.GetVolumeCapability().GetAccessType()).ToNot(BeNil())
			Expect(request.GetVolumeCapability().GetAccessMode().GetMode()).To(Equal(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER))
		})

		Context("When csi node server response some error", func() {
			BeforeEach(func() {
				ret := grpc.Errorf(codes.Internal, "Error mounting volume")
				fakeNodeClient.NodePublishVolumeReturns(nil, ret)
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

	Describe("#Unmount", func() {
		JustBeforeEach(func() {
			err = csiPlugin.Unmount(logger, "fakevolumeid")
		})

		Context("When csi node server unmount successful", func() {
			BeforeEach(func() {
				fakeNodeClient.NodeUnpublishVolumeReturns(&csi.NodeUnpublishVolumeResponse{}, nil)
			})

			It("should succeed", func() {
				Expect(err).NotTo(HaveOccurred())
				Expect(fakeNodeClient.NodeUnpublishVolumeCallCount()).To(Equal(1))
				Expect(conn.CloseCallCount()).To(Equal(1))
			})

			It("should unmount the right volume", func() {
				_, request, _ := fakeNodeClient.NodeUnpublishVolumeArgsForCall(0)
				Expect(request.GetVolumeId()).To(Equal("fakevolumeid"))
			})
		})

		Context("When csi node server unmount unsuccessful", func() {
			BeforeEach(func() {
				ret := grpc.Errorf(codes.Internal, "Error unmounting volume")
				fakeNodeClient.NodeUnpublishVolumeReturns(nil, ret)
			})

			It("report error and log it", func() {
				Expect(err).To(HaveOccurred())
				Expect(fakeNodeClient.NodeUnpublishVolumeCallCount()).To(Equal(1))
				Expect(logger.Buffer()).To(gbytes.Say("Error unmounting volume"))
				Expect(conn.CloseCallCount()).To(Equal(1))
			})
		})
	})

	Describe("#ListVolumes", func() {
		var (
			volumeId string
		)
		BeforeEach(func() {
			volumeId = "fakevolumeid"
			fakeNodeClient.NodePublishVolumeReturns(&csi.NodePublishVolumeResponse{}, nil)
			fakeNodeClient.NodeUnpublishVolumeReturns(&csi.NodeUnpublishVolumeResponse{}, nil)
		})

		Context("when a new volume get mounted", func() {
			var (
				err error
			)

			JustBeforeEach(func() {
				_, err = csiPlugin.Mount(logger, volumeId, map[string]interface{}{})
				Expect(err).ToNot(HaveOccurred())
			})

			It("should list the new volumes", func() {
				volumes, err := csiPlugin.ListVolumes(logger)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(volumes)).To(Equal(1))
				Expect(volumes).To(ContainElement(volumeId))
			})

			Context("when the same volume is mounted again", func() {
				JustBeforeEach(func() {
					_, err = csiPlugin.Mount(logger, volumeId, map[string]interface{}{})
					Expect(err).ToNot(HaveOccurred())
				})

				Context("when the volume gets unmounted", func() {
					JustBeforeEach(func() {
						err = csiPlugin.Unmount(logger, volumeId)
						Expect(err).ToNot(HaveOccurred())
					})

					It("should list the volume", func() {
						volumes, err := csiPlugin.ListVolumes(logger)
						Expect(err).ToNot(HaveOccurred())
						Expect(len(volumes)).To(Equal(1))
						Expect(volumes).To(ContainElement(volumeId))
					})

					Context("when the volume gets unmounted again", func() {
						JustBeforeEach(func() {
							err = csiPlugin.Unmount(logger, volumeId)
							Expect(err).ToNot(HaveOccurred())
						})

						It("should not list the volume", func() {
							volumes, err := csiPlugin.ListVolumes(logger)
							Expect(err).ToNot(HaveOccurred())
							Expect(len(volumes)).To(Equal(0))
						})
					})
				})
			})
		})

		Context("when mount and unmount are running in parallel", func() {
			It("should still list volumes correctly afterwards", func() {
				var wg sync.WaitGroup

				wg.Add(8)

				smash := func(volumeId string) {
					defer GinkgoRecover()
					defer wg.Done()
					for i := 0; i < 1000; i++ {
						_, err := csiPlugin.Mount(logger, volumeId, map[string]interface{}{})
						Expect(err).NotTo(HaveOccurred())

						r := rand.Intn(10)
						time.Sleep(time.Duration(r) * time.Microsecond)

						err = csiPlugin.Unmount(logger, volumeId)
						Expect(err).NotTo(HaveOccurred())

						r = rand.Intn(10)
						time.Sleep(time.Duration(r) * time.Microsecond)
					}
				}

				// Note go race detection should kick in if access is unsynchronized
				go smash("some-instance-1")
				go smash("some-instance-2")
				go smash("some-instance-3")
				go smash("some-instance-4")
				go smash("some-instance-5")
				go smash("some-instance-6")
				go smash("some-instance-7")
				go smash("some-instance-8")

				wg.Wait()

				volumes, err := csiPlugin.ListVolumes(logger)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(volumes)).To(Equal(0))
			})
		})
	})
})