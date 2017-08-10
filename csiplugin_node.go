package csiplugin

import (
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/volman"
	"github.com/paulcwarren/spec"
	"reflect"
)

type nodeWrapper struct {
	Impl interface{}
	Spec volman.PluginSpec
}

func (dw *nodeWrapper) GetImplementation() interface{} {
	return dw.Impl
}

func (dw *nodeWrapper) Matches(logger lager.Logger, otherSpec volman.PluginSpec) bool {
	logger = logger.Session("matches")
	logger.Info("start")
	defer logger.Info("end")

	matches := reflect.DeepEqual(dw.Spec, otherSpec)
	logger.Info("matches", lager.Data{"matches": matches})
	return matches
}

func NewCsiPlugin(plugin csi.NodeClient, pluginSpec volman.PluginSpec) volman.Plugin {
	return &nodeWrapper{
		Impl: plugin,
		Spec: pluginSpec,
	}
}
