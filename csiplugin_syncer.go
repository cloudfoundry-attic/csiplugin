package csiplugin

import (
	"github.com/tedsuo/ifrit"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/volman"
	"time"
	"code.cloudfoundry.org/clock"
	"os"
	"path/filepath"
	"code.cloudfoundry.org/goshims/filepathshim"
	"google.golang.org/grpc"
	"code.cloudfoundry.org/goshims/grpcshim"
	"github.com/paulcwarren/spec/csishim"
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
	csiShim 			 csishim.Csi
}

func NewCsiPluginSyncer(logger lager.Logger, pluginRegistry volman.PluginRegistry, pluginPaths []string, scanInterval time.Duration, clock clock.Clock) CsiPluginSyncer {
	return &csiPluginSyncer{
		logger: logger,
		scanInterval: scanInterval,
		clock: clock,
		pluginRegistry: pluginRegistry,
		pluginPaths: pluginPaths,
		filepathShim: &filepathshim.FilepathShim{},
		grpcShim: &grpcshim.GrpcShim{},
		csiShim: &csishim.CsiShim{},
	}
}

func NewCsiPluginSyncerWithShims(logger lager.Logger, pluginRegistry volman.PluginRegistry, pluginPaths []string, scanInterval time.Duration, clock clock.Clock, filepathShim filepathshim.Filepath, grpcShim grpcshim.Grpc, csiShim csishim.Csi) CsiPluginSyncer {
	return &csiPluginSyncer{
		logger: logger,
		scanInterval: scanInterval,
		clock: clock,
		pluginRegistry: pluginRegistry,
		pluginPaths: pluginPaths,
		filepathShim: filepathShim,
		grpcShim: grpcShim,
		csiShim: csiShim,
	}
}

func (p *csiPluginSyncer) Runner() ifrit.Runner {
	return p
}

func (p *csiPluginSyncer) Discover(logger lager.Logger) error {
	logger = logger.Session("discover")
	logger.Debug("start")
	logger.Info("discovering-csi-plugins", lager.Data{"plugin-paths": p.pluginPaths})
	defer logger.Debug("end")

	for _, pluginPath := range p.pluginPaths {
		pluginSpecFiles, err := filepath.Glob(pluginPath + "/*.json")
		if err!=nil {
			logger.Error("filepath-glob", err, lager.Data{"glob": pluginPath + "/*.json"})
			return err
		}
		for _, pluginSpecFile := range pluginSpecFiles {
			logger.Info("pluginSpecFile")
			pluginSpec, err := ReadSpec(logger, pluginSpecFile)
			if err!=nil {
				logger.Error("read-spec", err, lager.Data{"plugin-path": pluginPath, "plugin-spec-file": pluginSpecFile})
				return err
			}
			// instantiate a volman.Plugin implementation of a csi.NodePLugin
			conn, err := p.grpcShim.Dial(pluginSpec.Address, grpc.WithInsecure())

			if err != nil {
				logger.Error("grpc-dial", err, lager.Data{"address": pluginSpec.Address})
				return err
			}

			plugin := NewCsiPlugin(p.csiShim.NewNodeClient(conn))
			p.pluginRegistry.Set(map[string]volman.Plugin{pluginSpec.Name: plugin})
		}
	}
	return nil
}

func (p *csiPluginSyncer) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	logger := p.logger.Session("sync-csi-plugin")
	logger.Info("start")
	defer logger.Info("end")

	timer := p.clock.NewTimer(p.scanInterval)
	defer timer.Stop()

	err := p.Discover(logger)
	if err != nil {
		logger.Error("failed-discover", err)
		return err
	}

	close(ready)

	//newDriverCh := make(chan map[string]volman.Plugin, 1)

	for {
		select {
	//	case <-timer.C():
	//		go func() {
	//			drivers, err := r.Discover(logger)
	//			if err != nil {
	//				logger.Error("volman-driver-discovery-failed", err)
	//				newDriverCh <- nil
	//			} else {
	//				newDriverCh <- drivers
	//			}
	//		}()
	//
	//	case drivers := <-newDriverCh:
	//		r.setDockerDrivers(drivers)
	//		timer.Reset(r.scanInterval)
	//
		case signal := <-signals:
			logger.Info("received-signal", lager.Data{"signal": signal.String()})
			return nil
		}
	}
	return nil
}