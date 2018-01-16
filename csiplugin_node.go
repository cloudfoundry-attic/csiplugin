package csiplugin

import (
	"path"
	"reflect"
	"sync"
	"errors"
	"fmt"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"code.cloudfoundry.org/goshims/grpcshim"
	"code.cloudfoundry.org/goshims/osshim"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/volman"
	"code.cloudfoundry.org/csishim"
	"github.com/container-storage-interface/spec/lib/go/csi"
)

func csiVersion() *csi.Version {
	return &csi.Version{
		Major: 0,
		Minor: 1,
		Patch: 0,
	}
}

type nodeWrapper struct {
	Impl            interface{}
	Spec            volman.PluginSpec
	grpcShim        grpcshim.Grpc
	csiShim         csishim.Csi
	osShim          osshim.Os
	volumes         map[string]int
	volumesMutex    sync.RWMutex
	csiMountRootDir string
}

func (dw *nodeWrapper) Unmount(logger lager.Logger, volumeId string) error {
	logger = logger.Session("unmount")
	logger.Info("start")
	defer logger.Info("end")

	conn, err := dw.grpcShim.Dial(dw.Spec.Address, grpc.WithInsecure())
	defer conn.Close()
	if err != nil {
		logger.Error("grpc-dial", err, lager.Data{"address": dw.Spec.Address})
		return err
	}

	mountPath := path.Join(dw.csiMountRootDir, dw.Spec.Name)
	targetPath := path.Join(mountPath, volumeId)

	nodePlugin := dw.csiShim.NewNodeClient(conn)
	nodeResponse, err := nodePlugin.NodeUnpublishVolume(context.TODO(), &csi.NodeUnpublishVolumeRequest{
		Version:    csiVersion(),
		VolumeId:   volumeId,
		TargetPath: targetPath,
	})

	logger.Debug("node-response", lager.Data{"nodeResponse": nodeResponse})

	if err != nil {
		logger.Error("node-response-error", err)
		return err
	}

	dw.volumesMutex.Lock()
	defer dw.volumesMutex.Unlock()

	if count, ok := dw.volumes[volumeId]; ok {
		if count > 1 {
			dw.volumes[volumeId] = count - 1
		} else {
			delete(dw.volumes, volumeId)
		}
	}

	return nil
}

func (dw *nodeWrapper) Mount(logger lager.Logger, volumeId string, config map[string]interface{}) (volman.MountResponse, error) {
	logger = logger.Session("mount")
	logger.Info("start")
	defer logger.Info("end")

	conn, err := dw.grpcShim.Dial(dw.Spec.Address, grpc.WithInsecure())
	defer conn.Close()
	if err != nil {
		logger.Error("grpc-dial", err, lager.Data{"address": dw.Spec.Address})
		return volman.MountResponse{}, err
	}

	mountPath := path.Join(dw.csiMountRootDir, dw.Spec.Name)

	targetPath := path.Join(mountPath, volumeId)

	nodePlugin := dw.csiShim.NewNodeClient(conn)
	publishRequestVolID, ok := config["id"].(string)
	if !ok {
		err := errors.New(fmt.Sprintf("type assertion on VolumeId: not string, but %T", config["id"]))
		logger.Error("bind-config", err)
		return volman.MountResponse{}, err
	}
	volAttrs, ok := config["attributes"].(map[string]string)
	if !ok {
		err := errors.New(fmt.Sprintf("type assertion on VolumeAttributes: not map[string]string, but %T", config["attributes"]))
		logger.Error("bind-config", err)
		return volman.MountResponse{}, err
	}
	nodeResponse, err := nodePlugin.NodePublishVolume(context.TODO(), &csi.NodePublishVolumeRequest{
		Version:    csiVersion(),
		VolumeId:   publishRequestVolID,
		TargetPath: targetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
		VolumeAttributes: volAttrs,
	})

	logger.Debug("node-response", lager.Data{"nodeResponse": nodeResponse})

	if err != nil {
		logger.Error("node-response-error", err)
		return volman.MountResponse{}, err
	}

	dw.volumesMutex.Lock()
	defer dw.volumesMutex.Unlock()

	if count, ok := dw.volumes[publishRequestVolID]; ok {
		dw.volumes[publishRequestVolID] = count + 1
	} else {
		dw.volumes[publishRequestVolID] = 1
	}

	return volman.MountResponse{Path: targetPath}, nil
}

func (dw *nodeWrapper) ListVolumes(logger lager.Logger) ([]string, error) {
	logger = logger.Session("listvolumes")
	logger.Info("start")
	defer logger.Info("end")

	dw.volumesMutex.RLock()
	defer dw.volumesMutex.RUnlock()

	volumes := make([]string, len(dw.volumes))
	i := 0
	for volume := range dw.volumes {
		volumes[i] = volume
		i++
	}
	return volumes, nil
}

func (dw *nodeWrapper) Matches(logger lager.Logger, otherSpec volman.PluginSpec) bool {
	logger = logger.Session("matches")
	logger.Info("start")
	defer logger.Info("end")

	matches := reflect.DeepEqual(dw.Spec, otherSpec)
	logger.Info("matches", lager.Data{"matches": matches})
	return matches
}

func NewCsiPlugin(plugin csi.NodeClient, pluginSpec volman.PluginSpec, grpcShim grpcshim.Grpc, csiShim csishim.Csi, osShim osshim.Os, csiMountRootDir string) volman.Plugin {
	return &nodeWrapper{
		Impl:            plugin,
		Spec:            pluginSpec,
		grpcShim:        grpcShim,
		csiShim:         csiShim,
		osShim:          osShim,
		volumes:         map[string]int{},
		csiMountRootDir: csiMountRootDir,
	}
}
