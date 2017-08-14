package csiplugin

import (
	"code.cloudfoundry.org/lager"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func WriteSpec(logger lager.Logger, pluginsDirectory string, spec CsiPluginSpec) error {
	logger = logger.Session("write-csi-plugin-spec", lager.Data{"dir": pluginsDirectory, "name": spec.Name})
	logger.Info("start")
	defer logger.Info("end")

	err := os.MkdirAll(pluginsDirectory, 0666)
	if err != nil {
		logger.Error("error-creating-directory", err)
		return err
	}

	specFilePath := pluginsDirectory + "/" + spec.Name + ".json"
	logger.Info("spec-file", lager.Data{"path": specFilePath})

	f, err := os.Create(specFilePath)
	if err != nil {
		logger.Error("error-creating-file ", err)
		return err
	}
	defer f.Close()

	contents, err := json.Marshal(spec)
	if err != nil {
		logger.Error("error-marshalling-spec", err)
		return err
	}

	_, err = f.Write(contents)
	if err != nil {
		logger.Error("error-writing-file ", err)
		return err
	}
	f.Sync()
	return nil
}

func ReadSpec(logger lager.Logger, specFile string) (*CsiPluginSpec, error) {
	logger = logger.Session("read-csi-plugin-spec", lager.Data{"file-path": specFile})
	logger.Info("start")
	defer logger.Info("end")

	var pluginSpec CsiPluginSpec

	if strings.Contains(specFile, ".") {
		index := strings.LastIndex(specFile, ".")
		extension := specFile[index+1 : len(specFile)]
		switch extension {
		case "json":
			// extract url from json file
			configFile, err := os.Open(specFile)
			if err != nil {
				logger.Error("error-opening-config", err, lager.Data{"DriverFileName": specFile})
				return nil, err
			}
			jsonParser := json.NewDecoder(configFile)
			if err = jsonParser.Decode(&pluginSpec); err != nil {
				logger.Error("parsing-config-file-error", err)
				return nil, err
			}

		default:
			err := fmt.Errorf("unknown-spec-extension: %s", extension)
			logger.Error("spec", err)
			return nil, err
		}
	}

	return &pluginSpec, nil
}
