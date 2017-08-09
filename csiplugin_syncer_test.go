package csiplugin_test


import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/Kaixiang/csiplugin"
	"code.cloudfoundry.org/volman"
	"code.cloudfoundry.org/lager/lagertest"
	"code.cloudfoundry.org/volman/vollocal"
	"os"
	"io/ioutil"
	"time"
	"code.cloudfoundry.org/clock/fakeclock"
	"github.com/tedsuo/ifrit/ginkgomon"
	"github.com/tedsuo/ifrit"
	"fmt"
	"github.com/onsi/ginkgo/config"
	"code.cloudfoundry.org/goshims/filepathshim/filepath_fake"
	"code.cloudfoundry.org/goshims/grpcshim/grpc_fake"
	"code.cloudfoundry.org/goshims/filepathshim"
	"github.com/paulcwarren/spec/csishim/csi_fake"
)


var _ = Describe("CSIPluginSyncer", func() {
  var (
		syncer csiplugin.CsiPluginSyncer
		registry volman.PluginRegistry
		logger *lagertest.TestLogger
		defaultPluginsDirectory string
		scanInterval time.Duration
		fakeClock *fakeclock.FakeClock
		fakeFilePath *filepath_fake.FakeFilepath
		fakeGrpc *grpc_fake.FakeGrpc
		fakeCsi *csi_fake.FakeCsi
		process  ifrit.Process
		err error
	)

	BeforeEach(func() {
		defaultPluginsDirectory, err = ioutil.TempDir(os.TempDir(), "clienttest")
		Expect(err).ShouldNot(HaveOccurred())
		scanInterval = 10 * time.Second
		fakeClock = fakeclock.NewFakeClock(time.Unix(123, 456))
		fakeFilePath = &filepath_fake.FakeFilepath{}
		fakeGrpc = &grpc_fake.FakeGrpc{}
		fakeCsi = &csi_fake.FakeCsi{}

		logger = lagertest.NewTestLogger("csi-plugin-syncer-test")
		registry = vollocal.NewPluginRegistry()
	})

	JustBeforeEach(func() {
		syncer = csiplugin.NewCsiPluginSyncerWithShims(logger, registry, []string{defaultPluginsDirectory}, scanInterval, fakeClock, &filepathshim.FilepathShim{}, fakeGrpc, fakeCsi)
	})

	Describe("#Runner", func() {
		It("has a non-nil runner", func() {
			Expect(syncer.Runner()).NotTo(BeNil())
		})

		It("has a non-nil and empty driver registry", func() {
			Expect(registry).NotTo(BeNil())
			Expect(len(registry.Plugins())).To(Equal(0))
		})
	})


	Describe("#Run", func() {

		JustBeforeEach(func() {
			process = ginkgomon.Invoke(syncer.Runner())
		})

		AfterEach(func() {
			ginkgomon.Kill(process)
		})

		Context("when there are no csi plugins", func() {
			It("should add nothing to the plugin registry", func() {
				drivers := registry.Plugins()
				Expect(len(drivers)).To(Equal(0))
			})
		})

		Context("given there is a csi plugin to be discovered", func() {
			var (
				driverName string
				address string
			)
			BeforeEach(func() {
				driverName = fmt.Sprintf("csi-plugin-%d", config.GinkgoConfig.ParallelNode)
				address = "127.0.0.1:50051"
				spec := csiplugin.CsiPluginSpec{
					Name: driverName,
					Address: address,
				}
				err := csiplugin.WriteSpec(logger, defaultPluginsDirectory, spec)
				Expect(err).NotTo(HaveOccurred())
			})

			Context("given the node is available", func() {
				BeforeEach(func() {

				})

				It("should have discover it and add it to the plugin registry", func() {
					Expect(fakeGrpc.DialCallCount()).To(Equal(1))
					actualAddr, _ := fakeGrpc.DialArgsForCall(0)
					Expect(actualAddr).To(Equal(address))
					drivers := registry.Plugins()
					Expect(len(drivers)).To(Equal(1))
					_, pluginFound := drivers[driverName]
					Expect(pluginFound).To(Equal(true))
				})
			})
		})
	})
})