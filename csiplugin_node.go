package csiplugin

import (
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/volman"
	"github.com/paulcwarren/spec"
)

type nodeWrapper struct {
	Impl interface{}
}

func (dw *nodeWrapper) GetImplementation() interface{} {
	return dw.Impl
}

func (dw *nodeWrapper) Matches(logger lager.Logger, pluginSpec volman.PluginSpec) bool {
	logger = logger.Session("matches")
	logger.Info("start")
	defer logger.Info("end")

	var matches bool
	//matchableDriver, ok := dw.Driver.(MatchableDriver)
	//logger.Info("matches", lager.Data{"is-matchable": ok})
	//if ok {
	//	var tlsConfig *TLSConfig
	//	if pluginSpec.TLSConfig != nil {
	//		tlsConfig = &TLSConfig{
	//			InsecureSkipVerify: pluginSpec.TLSConfig.InsecureSkipVerify,
	//			CAFile: pluginSpec.TLSConfig.CAFile,
	//			CertFile: pluginSpec.TLSConfig.CertFile,
	//			KeyFile: pluginSpec.TLSConfig.KeyFile,
	//		}
	//	}
	//	matches = matchableDriver.Matches(logger, pluginSpec.Address, tlsConfig)
	//}
	logger.Info("matches", lager.Data{"matches": matches})
	return matches
}

func NewCsiPlugin(plugin csi.NodeClient) volman.Plugin {
	return &nodeWrapper{
		Impl: plugin,
	}
}
