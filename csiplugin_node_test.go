package csiplugin_test

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"

	"code.cloudfoundry.org/csiplugin"
	"code.cloudfoundry.org/csiplugin/csipluginfakes"
	"code.cloudfoundry.org/csiplugin/oshelper"
	"code.cloudfoundry.org/csishim/csi_fake"
	"code.cloudfoundry.org/dockerdriver/dockerdriverfakes"
	"code.cloudfoundry.org/goshims/grpcshim/grpc_fake"
	"code.cloudfoundry.org/goshims/osshim/os_fake"
	"code.cloudfoundry.org/lager/lagertest"
	"code.cloudfoundry.org/volman"
	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"

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
		mountPath      string
		config         map[string]interface{}
		fakeInvoker    *dockerdriverfakes.FakeInvoker
		fakeBgInvoker  *csipluginfakes.FakeBackgroundInvoker
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
		mountPath = path.Join(volumesRootDir, "mounts", "fakecsi")
		fakeInvoker = &dockerdriverfakes.FakeInvoker{}
		fakeBgInvoker = &csipluginfakes.FakeBackgroundInvoker{}
		csiPlugin = csiplugin.NewCsiPluginWithInvoker(fakeInvoker, fakeBgInvoker, fakeNodeClient, fakePluginSpec, fakeGrpc, fakeCsi, fakeOs, volumesRootDir, oshelper.NewOsHelper())
		conn = new(grpc_fake.FakeClientConn)
		fakeGrpc.DialReturns(conn, nil)
		config = map[string]interface{}{"id": "fakevolumeid", "attributes": map[string]interface{}{"foo": "bar"}}
	})

	Describe("#Mount", func() {
		JustBeforeEach(func() {
			mountResponse, err = csiPlugin.Mount(logger, "fakevolumeid", config)
		})

		BeforeEach(func() {
			fakeNodeClient.NodePublishVolumeReturns(&csi.NodePublishVolumeResponse{}, nil)
		})

		It("should mount the right volume", func() {
			_, request, _ := fakeNodeClient.NodePublishVolumeArgsForCall(0)
			Expect(request.GetVolumeId()).To(Equal("fakevolumeid"))
			Expect(request.GetVolumeCapability().GetAccessType()).ToNot(BeNil())
			Expect(request.GetVolumeCapability().GetAccessMode().GetMode()).To(Equal(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER))
			Expect(request.GetVolumeAttributes()).To(Equal(map[string]string{"foo": "bar"}))
		})

		Context("when doing the same mount for the second time", func() {
			BeforeEach(func() {
				mountResponse, err = csiPlugin.Mount(logger, "fakevolumeid", config)
			})

			It("should only mount once", func() {
				Expect(fakeNodeClient.NodePublishVolumeCallCount()).To(Equal(1))
			})
		})

		Context("when the mount path doesn't exist", func() {
			BeforeEach(func() {
				fakeOs.StatReturns(nil, os.ErrNotExist)
			})

			It("should prepare the mount path", func() {
				mkdirpath, _ := fakeOs.MkdirAllArgsForCall(0)
				Expect(mkdirpath).To(Equal(path.Join(mountPath, "fakevolumeid")))
			})
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

		Context("when the id from the bind config is the wrong type", func() {
			BeforeEach(func() {
				config = map[string]interface{}{"id": 123}
			})

			It("should fail with a type assertion error", func() {
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("type assertion on VolumeId: not string, but int"))
			})

		})

		Context("when attributes from the bind config is the wrong type", func() {
			BeforeEach(func() {
				config = map[string]interface{}{"id": "abcd", "attributes": map[string]int{"test": 1}}
			})

			It("should fail with a type assertion error", func() {
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("json: cannot unmarshal number into Go value of type string"))
			})
		})

		Context("when the attributes are nil", func() {
			BeforeEach(func() {
				config = map[string]interface{}{"id": "abcd", "attributes": nil}
			})

			It("should succeed", func() {
				Expect(err).ToNot(HaveOccurred())
			})
		})
	})

	Describe("#Unmount", func() {
		Context("when volume id doesn't have a csi volume attached", func() {
			JustBeforeEach(func() {
				err = csiPlugin.Unmount(logger, "relevant-id")
			})

			It("should fail", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when volume id have a csi volume attached", func() {
			BeforeEach(func() {
				config = map[string]interface{}{"id": "fakevolumeid", "attributes": map[string]string{"foo": "bar"}}
				_, err = csiPlugin.Mount(logger, "relevant-id", config)
				Expect(err).NotTo(HaveOccurred())
				fakeNodeClient.NodePublishVolumeReturns(&csi.NodePublishVolumeResponse{}, nil)
			})

			JustBeforeEach(func() {
				err = csiPlugin.Unmount(logger, "relevant-id")
			})

			Context("When csi node server unmount successful", func() {
				BeforeEach(func() {
					fakeNodeClient.NodeUnpublishVolumeReturns(&csi.NodeUnpublishVolumeResponse{}, nil)
				})

				It("should succeed", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(fakeNodeClient.NodeUnpublishVolumeCallCount()).To(Equal(1))
					Expect(conn.CloseCallCount()).To(Equal(2))
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
					Expect(conn.CloseCallCount()).To(Equal(2))
				})
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

				_, err = csiPlugin.Mount(logger, volumeId, config)
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
					_, err = csiPlugin.Mount(logger, volumeId, config)
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

				smash := func(csiVolumeId string) {
					defer GinkgoRecover()
					defer wg.Done()
					smashConfig := map[string]interface{}{"id": csiVolumeId, "attributes": map[string]string{"foo": "bar"}}
					for i := 0; i < 1000; i++ {

						// let's test reference counting whilst we are at it!
						mountCount := rand.Intn(9) + 1
						for i := 0; i < mountCount; i++ {
							_, errM := csiPlugin.Mount(logger, fmt.Sprintf("binding-id-%s", csiVolumeId), smashConfig)
							Expect(errM).NotTo(HaveOccurred())
						}

						r := rand.Intn(10)
						time.Sleep(time.Duration(r) * time.Microsecond)

						for i := 0; i < mountCount; i++ {
							errU := csiPlugin.Unmount(logger, fmt.Sprintf("binding-id-%s", csiVolumeId))
							Expect(errU).NotTo(HaveOccurred())
						}

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

				Expect(fakeNodeClient.NodeUnpublishVolumeCallCount()).To(Equal(1000 * 8))

			})

		})
	})
})
