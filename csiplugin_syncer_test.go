package csiplugin_test

import (
	"code.cloudfoundry.org/clock/fakeclock"
	"code.cloudfoundry.org/goshims/filepathshim"
	"code.cloudfoundry.org/goshims/filepathshim/filepath_fake"
	"code.cloudfoundry.org/goshims/grpcshim/grpc_fake"
	"code.cloudfoundry.org/lager/lagertest"
	"code.cloudfoundry.org/volman"
	"code.cloudfoundry.org/volman/vollocal"
	"errors"
	"fmt"
	"github.com/Kaixiang/csiplugin"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
	"github.com/paulcwarren/spec"
	"github.com/paulcwarren/spec/csishim/csi_fake"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"
	"io/ioutil"
	"os"
	"time"
)

var _ = Describe("CSIPluginSyncer", func() {
	var (
		syncer                 csiplugin.CsiPluginSyncer
		registry               volman.PluginRegistry
		logger                 *lagertest.TestLogger
		firstPluginsDirectory  string
		secondPluginsDirectory string
		scanInterval           time.Duration
		fakeClock              *fakeclock.FakeClock
		fakeFilePath           *filepath_fake.FakeFilepath
		fakeGrpc               *grpc_fake.FakeGrpc
		fakeCsi                *csi_fake.FakeCsi
		fakeNodePlugin         *csi_fake.FakeNodeClient
		pluginPaths            []string
		process                ifrit.Process
		err                    error
	)

	BeforeEach(func() {
		firstPluginsDirectory, err = ioutil.TempDir(os.TempDir(), "one")
		secondPluginsDirectory, err = ioutil.TempDir(os.TempDir(), "two")
		Expect(err).ShouldNot(HaveOccurred())
		scanInterval = 10 * time.Second
		fakeClock = fakeclock.NewFakeClock(time.Unix(123, 456))
		fakeFilePath = &filepath_fake.FakeFilepath{}
		fakeGrpc = &grpc_fake.FakeGrpc{}
		fakeCsi = &csi_fake.FakeCsi{}
		fakeNodePlugin = &csi_fake.FakeNodeClient{}
		pluginPaths = []string{firstPluginsDirectory}

		logger = lagertest.NewTestLogger("csi-plugin-syncer-test")
		registry = vollocal.NewPluginRegistry()
	})

	JustBeforeEach(func() {
		syncer = csiplugin.NewCsiPluginSyncerWithShims(logger, registry, pluginPaths, scanInterval, fakeClock, &filepathshim.FilepathShim{}, fakeGrpc, fakeCsi)
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
				address    string
			)
			BeforeEach(func() {
				driverName = fmt.Sprintf("csi-plugin-%d", config.GinkgoConfig.ParallelNode)
				address = "127.0.0.1:50051"
				spec := csiplugin.CsiPluginSpec{
					Name:    driverName,
					Address: address,
				}
				err := csiplugin.WriteSpec(logger, firstPluginsDirectory, spec)
				Expect(err).NotTo(HaveOccurred())

				fakeCsi.NewNodeClientReturns(fakeNodePlugin)
			})

			Context("given the node is available", func() {
				BeforeEach(func() {
					fakeNodePlugin.ProbeNodeReturns(&csi.ProbeNodeResponse{}, nil)
				})

				It("should have discover it and add it to the plugin registry", func() {
					Expect(fakeGrpc.DialCallCount()).To(Equal(1))
					actualAddr, _ := fakeGrpc.DialArgsForCall(0)
					Expect(actualAddr).To(Equal(address))

					Expect(fakeNodePlugin.ProbeNodeCallCount()).To(Equal(1))

					plugins := registry.Plugins()
					Expect(len(plugins)).To(Equal(1))

					_, pluginFound := plugins[driverName]
					Expect(pluginFound).To(Equal(true))
				})

				Context("given the scan interval then elapses", func() {

					It("should re-discover but not update the plugin registry", func() {
						fakeClock.Increment(11 * time.Second)

						Consistently(registry.Plugins(), 2).Should(HaveLen(1))
						Expect(fakeCsi.NewNodeClientCallCount()).To(Equal(1))
					})
				})
			})

			Context("given the node is not available", func() {
				BeforeEach(func() {
					fakeNodePlugin.ProbeNodeReturns(nil, errors.New("connection-refused"))
				})

				It("should have discover it and add it to the plugin registry", func() {
					Expect(fakeGrpc.DialCallCount()).To(Equal(1))
					actualAddr, _ := fakeGrpc.DialArgsForCall(0)
					Expect(actualAddr).To(Equal(address))
					drivers := registry.Plugins()

					Expect(fakeNodePlugin.ProbeNodeCallCount()).To(Equal(1))

					Expect(len(drivers)).To(Equal(0))
				})
			})
		})
	})

	Describe("#Discover", func() {
		JustBeforeEach(func() {
			syncer.Discover(logger)
		})

		Context("given a single plugin path", func() {

			Context("given there are no csi plugins", func() {
				It("should add nothing to the plugin registry", func() {
					drivers := registry.Plugins()
					Expect(len(drivers)).To(Equal(0))
				})
			})

			Context("given there is a csi plugin to be discovered", func() {
				var (
					driverName string
					address    string
				)

				BeforeEach(func() {
					driverName = fmt.Sprintf("csi-plugin-%d", config.GinkgoConfig.ParallelNode)
					address = "127.0.0.1:50051"
					spec := csiplugin.CsiPluginSpec{
						Name:    driverName,
						Address: address,
					}
					err := csiplugin.WriteSpec(logger, firstPluginsDirectory, spec)
					Expect(err).NotTo(HaveOccurred())

					fakeCsi.NewNodeClientReturns(fakeNodePlugin)
				})

				Context("given the node is available", func() {
					BeforeEach(func() {
						fakeNodePlugin.ProbeNodeReturns(&csi.ProbeNodeResponse{}, nil)
					})

					It("should discover it and add it to the plugin registry", func() {
						Expect(fakeGrpc.DialCallCount()).To(Equal(1))
						actualAddr, _ := fakeGrpc.DialArgsForCall(0)
						Expect(actualAddr).To(Equal(address))

						Expect(fakeNodePlugin.ProbeNodeCallCount()).To(Equal(1))

						plugins := registry.Plugins()
						Expect(len(plugins)).To(Equal(1))

						_, pluginFound := plugins[driverName]
						Expect(pluginFound).To(Equal(true))
					})

					Context("given re-discovery", func() {
						JustBeforeEach(func() {
							syncer.Discover(logger)
						})

						It("should not update the plugin registry", func() {
							Expect(registry.Plugins()).To(HaveLen(1))
							Expect(fakeCsi.NewNodeClientCallCount()).To(Equal(1))
						})
					})

					Context("when the plugin's spec is changed", func() {
						var (
							updatedAddress string
						)
						JustBeforeEach(func() {
							updatedAddress = "127.0.0.1:99999"
							spec := csiplugin.CsiPluginSpec{
								Name:    driverName,
								Address: updatedAddress,
							}
							err := csiplugin.WriteSpec(logger, firstPluginsDirectory, spec)
							Expect(err).NotTo(HaveOccurred())

							syncer.Discover(logger)
							Expect(registry.Plugins()).To(HaveLen(1))
						})

						It("should re-discover the plugin and update the registry", func() {
							Expect(fakeGrpc.DialCallCount()).To(Equal(2))
							actualAddr1, _ := fakeGrpc.DialArgsForCall(0)
							Expect(actualAddr1).To(Equal(address))
							actualAddr2, _ := fakeGrpc.DialArgsForCall(1)
							Expect(actualAddr2).To(Equal(updatedAddress))

							Expect(fakeCsi.NewNodeClientCallCount()).To(Equal(2))
							Expect(fakeNodePlugin.ProbeNodeCallCount()).To(Equal(2))

							plugins := registry.Plugins()
							Expect(len(plugins)).To(Equal(1))

							_, pluginFound := plugins[driverName]
							Expect(pluginFound).To(Equal(true))
						})
					})
				})

				Context("given the node is not available", func() {
					BeforeEach(func() {
						fakeNodePlugin.ProbeNodeReturns(nil, errors.New("connection-refused"))
					})

					It("should have discover it and add it to the plugin registry", func() {
						Expect(fakeGrpc.DialCallCount()).To(Equal(1))
						actualAddr, _ := fakeGrpc.DialArgsForCall(0)
						Expect(actualAddr).To(Equal(address))
						drivers := registry.Plugins()

						Expect(fakeNodePlugin.ProbeNodeCallCount()).To(Equal(1))

						Expect(len(drivers)).To(Equal(0))
					})
				})
			})
		})

		Context("given more than one plugin path", func() {

			BeforeEach(func() {
				pluginPaths = []string{firstPluginsDirectory, secondPluginsDirectory}
			})

			Context("given multiple plugins to be discovered, in multiple directories", func() {
				var (
					pluginName  string
					address     string
					spec        csiplugin.CsiPluginSpec
					pluginName2 string
					address2    string
					spec2       csiplugin.CsiPluginSpec
					err         error
				)

				BeforeEach(func() {
					pluginName = fmt.Sprintf("csi-plugin-%d", config.GinkgoConfig.ParallelNode)
					address = "127.0.0.1:50051"
					spec = csiplugin.CsiPluginSpec{
						Name:    pluginName,
						Address: address,
					}
					err = csiplugin.WriteSpec(logger, firstPluginsDirectory, spec)
					Expect(err).NotTo(HaveOccurred())

					pluginName2 = fmt.Sprintf("csi-plugin-2-%d", config.GinkgoConfig.ParallelNode)
					address2 = "127.0.0.1:50061"
					spec2 = csiplugin.CsiPluginSpec{
						Name:    pluginName2,
						Address: address2,
					}
					err = csiplugin.WriteSpec(logger, secondPluginsDirectory, spec2)
					Expect(err).NotTo(HaveOccurred())

					// make both plugins active
					fakeCsi.NewNodeClientReturns(fakeNodePlugin)
					fakeNodePlugin.ProbeNodeReturns(&csi.ProbeNodeResponse{}, nil)
				})

				It("should discover both plugins", func() {
					plugins := registry.Plugins()
					Expect(len(plugins)).To(Equal(2))

					_, pluginFound := plugins[pluginName]
					Expect(pluginFound).To(Equal(true))

					_, plugin2Found := plugins[pluginName2]
					Expect(plugin2Found).To(Equal(true))
				})
			})

			Context("given the same plugin in multiple directories", func() {
				var (
					pluginName string
					address    string
					spec       csiplugin.CsiPluginSpec
					err        error
				)

				BeforeEach(func() {
					pluginName = fmt.Sprintf("csi-plugin-%d", config.GinkgoConfig.ParallelNode)
					address = "127.0.0.1:50051"
					spec = csiplugin.CsiPluginSpec{
						Name:    pluginName,
						Address: address,
					}
					err = csiplugin.WriteSpec(logger, firstPluginsDirectory, spec)
					Expect(err).NotTo(HaveOccurred())

					fmt.Sprintf("csi-plugin-%d", config.GinkgoConfig.ParallelNode)
					err = csiplugin.WriteSpec(logger, secondPluginsDirectory, spec)
					Expect(err).NotTo(HaveOccurred())

					// make both plugins active
					fakeCsi.NewNodeClientReturns(fakeNodePlugin)
					fakeNodePlugin.ProbeNodeReturns(&csi.ProbeNodeResponse{}, nil)
				})

				It("should discover the plugin and add it to the registry once only", func() {
					plugins := registry.Plugins()
					Expect(len(plugins)).To(Equal(1))

					_, pluginFound := plugins[pluginName]
					Expect(pluginFound).To(Equal(true))
				})
			})
		})
	})
})
