package csiplugin

import (
	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/goshims/filepathshim"
	"code.cloudfoundry.org/goshims/grpcshim"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/volman"
	"fmt"
	"github.com/paulcwarren/spec"
	"github.com/paulcwarren/spec/csishim"
	"github.com/tedsuo/ifrit"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"os"
	"path/filepath"
	"time"
)

type CsiPluginSyncer interface {
	Runner() ifrit.Runner
	Discover(logger lager.Logger) error
}

type csiPluginSyncer struct {
	logger         lager.Logger
	scanInterval   time.Duration
	clock          clock.Clock
	pluginRegistry volman.PluginRegistry
	pluginPaths    []string
	filepathShim   filepathshim.Filepath
	grpcShim       grpcshim.Grpc
	csiShim        csishim.Csi
}

func NewCsiPluginSyncer(logger lager.Logger, pluginRegistry volman.PluginRegistry, pluginPaths []string, scanInterval time.Duration, clock clock.Clock) CsiPluginSyncer {
	return &csiPluginSyncer{
		logger:         logger,
		scanInterval:   scanInterval,
		clock:          clock,
		pluginRegistry: pluginRegistry,
		pluginPaths:    pluginPaths,
		filepathShim:   &filepathshim.FilepathShim{},
		grpcShim:       &grpcshim.GrpcShim{},
		csiShim:        &csishim.CsiShim{},
	}
}

func NewCsiPluginSyncerWithShims(logger lager.Logger, pluginRegistry volman.PluginRegistry, pluginPaths []string, scanInterval time.Duration, clock clock.Clock, filepathShim filepathshim.Filepath, grpcShim grpcshim.Grpc, csiShim csishim.Csi) CsiPluginSyncer {
	return &csiPluginSyncer{
		logger:         logger,
		scanInterval:   scanInterval,
		clock:          clock,
		pluginRegistry: pluginRegistry,
		pluginPaths:    pluginPaths,
		filepathShim:   filepathShim,
		grpcShim:       grpcShim,
		csiShim:        csiShim,
	}
}

func (p *csiPluginSyncer) Runner() ifrit.Runner {
	return p
}

func (p *csiPluginSyncer) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	logger := p.logger.Session("sync-csi-plugin")
	logger.Info("start")
	defer logger.Info("end")

	err := p.Discover(logger)
	if err != nil {
		logger.Error("failed-discover", err)
		return err
	}

	timer := p.clock.NewTimer(p.scanInterval)
	defer timer.Stop()

	close(ready)

	for {
		select {
		case <-timer.C():
			go func() {
				err := p.Discover(logger)
				if err != nil {
					logger.Error("volman-driver-discovery-failed", err)
				} else {
				}
				logger.Info("reset-timer")
				timer.Reset(p.scanInterval)
			}()

		case signal := <-signals:
			logger.Info("received-signal", lager.Data{"signal": signal.String()})
			return nil
		}
	}
	return nil
}

func (p *csiPluginSyncer) Discover(logger lager.Logger) error {
	logger = logger.Session("discover")
	logger.Debug("start")
	logger.Info("discovering-csi-plugins", lager.Data{"plugin-paths": p.pluginPaths})
	defer logger.Debug("end")

	plugins := map[string]volman.Plugin{}

	for _, pluginPath := range p.pluginPaths {
		pluginSpecFiles, err := filepath.Glob(pluginPath + "/*.json")
		if err != nil {
			logger.Error("filepath-glob", err, lager.Data{"glob": pluginPath + "/*.json"})
			return err
		}
		for _, pluginSpecFile := range pluginSpecFiles {
			csiPluginSpec, err := ReadSpec(logger, pluginSpecFile)
			if err != nil {
				logger.Error("read-spec-failed", err, lager.Data{"plugin-path": pluginPath, "plugin-spec-file": pluginSpecFile})
				continue
			}

			existingPlugin, found := p.pluginRegistry.Plugins()[csiPluginSpec.Name]
			pluginSpec := volman.PluginSpec{
				Name:    csiPluginSpec.Name,
				Address: csiPluginSpec.Address,
			}

			if !found || !existingPlugin.Matches(logger, pluginSpec) {
				logger.Info("new-plugin", lager.Data{"name": pluginSpec.Name, "address": pluginSpec.Address})

				// instantiate a volman.Plugin implementation of a csi.NodePlugin
				conn, err := p.grpcShim.Dial(csiPluginSpec.Address, grpc.WithInsecure())

				if err != nil {
					logger.Error("grpc-dial", err, lager.Data{"address": csiPluginSpec.Address})
					return err
				}

				nodePlugin := p.csiShim.NewNodeClient(conn)
				_, err = nodePlugin.ProbeNode(context.TODO(), &csi.ProbeNodeRequest{
					Version: &csi.Version{
						Major: 0,
						Minor: 0,
						Patch: 1,
					},
				})
				if err != nil {
					logger.Info("probe-node-unresponsive", lager.Data{"name": csiPluginSpec.Name, "address": csiPluginSpec.Address})
					continue
				}

				plugin := NewCsiPlugin(nodePlugin, pluginSpec)
				plugins[csiPluginSpec.Name] = plugin
			} else {
				logger.Info("discovered-plugin-ignored", lager.Data{"name": pluginSpec.Name, "address": pluginSpec.Address})
				plugins[csiPluginSpec.Name] = existingPlugin
			}
		}
		p.pluginRegistry.Set(plugins)
	}
	return nil
}
