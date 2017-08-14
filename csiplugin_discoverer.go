package csiplugin

import (
	"code.cloudfoundry.org/goshims/filepathshim"
	"code.cloudfoundry.org/goshims/grpcshim"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/volman"
	"github.com/paulcwarren/spec"
	"github.com/paulcwarren/spec/csishim"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"path/filepath"
)

type csiPluginDiscoverer struct {
	logger         lager.Logger
	pluginRegistry volman.PluginRegistry
	pluginPaths    []string
	filepathShim   filepathshim.Filepath
	grpcShim       grpcshim.Grpc
	csiShim        csishim.Csi
}

func NewCsiPluginDiscoverer(logger lager.Logger, pluginRegistry volman.PluginRegistry, pluginPaths []string) volman.Discoverer {
	return &csiPluginDiscoverer{
		logger:         logger,
		pluginRegistry: pluginRegistry,
		pluginPaths:    pluginPaths,
		filepathShim:   &filepathshim.FilepathShim{},
		grpcShim:       &grpcshim.GrpcShim{},
		csiShim:        &csishim.CsiShim{},
	}
}

func NewCsiPluginDiscovererWithShims(logger lager.Logger, pluginRegistry volman.PluginRegistry, pluginPaths []string, filepathShim filepathshim.Filepath, grpcShim grpcshim.Grpc, csiShim csishim.Csi) volman.Discoverer {
	return &csiPluginDiscoverer{
		logger:         logger,
		pluginRegistry: pluginRegistry,
		pluginPaths:    pluginPaths,
		filepathShim:   filepathShim,
		grpcShim:       grpcShim,
		csiShim:        csiShim,
	}
}

func (p *csiPluginDiscoverer) Discover(logger lager.Logger) (map[string]volman.Plugin, error) {
	logger = logger.Session("discover")
	logger.Debug("start")
	logger.Info("discovering-csi-plugins", lager.Data{"plugin-paths": p.pluginPaths})
	defer logger.Debug("end")

	plugins := map[string]volman.Plugin{}

	for _, pluginPath := range p.pluginPaths {
		pluginSpecFiles, err := filepath.Glob(pluginPath + "/*.json")
		if err != nil {
			logger.Error("filepath-glob", err, lager.Data{"glob": pluginPath + "/*.json"})
			return plugins, err
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
					return plugins, err
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

		logger.Info("csi-discover-start")
	}
	return plugins, nil
}