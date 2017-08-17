package csiplugin

import (
	"errors"
	"fmt"
	"reflect"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"code.cloudfoundry.org/goshims/grpcshim"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/volman"
	"github.com/paulcwarren/spec"
	"github.com/paulcwarren/spec/csishim"
)

type nodeWrapper struct {
	Impl           interface{}
	Spec           volman.PluginSpec
	grpcShim       grpcshim.Grpc
	csiShim        csishim.Csi
	volumesRootDir string
}

func (dw *nodeWrapper) GetImplementation() interface{} {
	return dw.Impl
}

func (dw *nodeWrapper) Mount(logger lager.Logger, driverId string, volumeId string, config map[string]interface{}) (volman.MountResponse, error) {
	logger = logger.Session("mount")
	logger.Info("start")
	defer logger.Info("end")

	conn, err := dw.grpcShim.Dial(dw.Spec.Address, grpc.WithInsecure())
	defer conn.Close()
	if err != nil {
		logger.Error("grpc-dial", err, lager.Data{"address": dw.Spec.Address})
		return volman.MountResponse{}, err
	}

	// Diego executor can only bind mount path starting with /var/vcap/data?
	targetPath := dw.volumesRootDir + "/" + volumeId
	volId := &csi.VolumeID{Values: map[string]string{"volume_name": volumeId}}

	nodePlugin := dw.csiShim.NewNodeClient(conn)
	nodeResponse, err := nodePlugin.NodePublishVolume(context.TODO(), &csi.NodePublishVolumeRequest{
		Version: &csi.Version{
			Major: 0,
			Minor: 0,
			Patch: 1,
		},
		VolumeId:   volId,
		TargetPath: targetPath,
		VolumeCapability: &csi.VolumeCapability{
			Value: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{MountFlags: []string{}}},
		},
		Readonly: false,
	})

	logger.Debug(fmt.Sprintf("nodeResponse: %#v", nodeResponse))

	if nodeResponse.GetError() != nil {
		logger.Error("node-response-error", err, lager.Data{"Error": nodeResponse.GetError().String()})
		return volman.MountResponse{}, errors.New(nodeResponse.GetError().String())
	}

	return volman.MountResponse{Path: targetPath}, nil
}

func (dw *nodeWrapper) Matches(logger lager.Logger, otherSpec volman.PluginSpec) bool {
	logger = logger.Session("matches")
	logger.Info("start")
	defer logger.Info("end")

	matches := reflect.DeepEqual(dw.Spec, otherSpec)
	logger.Info("matches", lager.Data{"matches": matches})
	return matches
}

func NewCsiPlugin(plugin csi.NodeClient, pluginSpec volman.PluginSpec, grpcShim grpcshim.Grpc, csiShim csishim.Csi, volumesRootDir string) volman.Plugin {
	return &nodeWrapper{
		Impl:           plugin,
		Spec:           pluginSpec,
		grpcShim:       grpcShim,
		csiShim:        csiShim,
		volumesRootDir: volumesRootDir,
	}
}
