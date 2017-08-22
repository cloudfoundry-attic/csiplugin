package csiplugin

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"code.cloudfoundry.org/goshims/grpcshim"
	"code.cloudfoundry.org/goshims/osshim"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/volman"
	"github.com/paulcwarren/spec"
	"github.com/paulcwarren/spec/csishim"
)

func csiVersion() *csi.Version {
	return &csi.Version{
		Major: 0,
		Minor: 0,
		Patch: 1,
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

func (dw *nodeWrapper) GetImplementation() interface{} {
	return dw.Impl
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
	volId := &csi.VolumeID{Values: map[string]string{"volume_name": volumeId}}

	nodePlugin := dw.csiShim.NewNodeClient(conn)
	nodeResponse, err := nodePlugin.NodeUnpublishVolume(context.TODO(), &csi.NodeUnpublishVolumeRequest{
		Version:    csiVersion(),
		VolumeId:   volId,
		TargetPath: targetPath,
	})

	logger.Debug(fmt.Sprintf("nodeResponse: %#v", nodeResponse))

	if nodeResponse.GetError() != nil {
		logger.Error("node-response-error", err, lager.Data{"Error": nodeResponse.GetError().String()})
		return errors.New(nodeResponse.GetError().String())
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
	logger.Debug(fmt.Sprintf("reference count: %#v", dw.volumes))

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
	err = createVolumesRootifNotExist(logger, mountPath, dw.osShim)
	if err != nil {
		logger.Error("create-volumes-root", err)
		return volman.MountResponse{}, err
	}

	targetPath := path.Join(mountPath, volumeId)
	volId := &csi.VolumeID{Values: map[string]string{"volume_name": volumeId}}

	nodePlugin := dw.csiShim.NewNodeClient(conn)
	nodeResponse, err := nodePlugin.NodePublishVolume(context.TODO(), &csi.NodePublishVolumeRequest{
		Version:    csiVersion(),
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

	dw.volumesMutex.Lock()
	defer dw.volumesMutex.Unlock()

	if count, ok := dw.volumes[volumeId]; ok {
		dw.volumes[volumeId] = count + 1
	} else {
		dw.volumes[volumeId] = 1
	}

	logger.Debug(fmt.Sprintf("reference count: %#v", dw.volumes))
	return volman.MountResponse{Path: targetPath}, nil
}

func createVolumesRootifNotExist(logger lager.Logger, mountPath string, osShim osshim.Os) error {
	mountPath, err := filepath.Abs(mountPath)
	if err != nil {
		logger.Fatal("abs-failed", err)
	}

	logger.Debug(mountPath)
	_, err = osShim.Stat(mountPath)

	if err != nil {
		if osShim.IsNotExist(err) {
			// Create the directory if not exist
			orig := syscall.Umask(000)
			defer syscall.Umask(orig)
			err = osShim.MkdirAll(mountPath, os.ModePerm)
			if err != nil {
				logger.Error("mkdirall", err)
				return err
			}
		}
	}
	return nil
}

func (dw *nodeWrapper) ListVolumes(logger lager.Logger) ([]string, error) {
	logger = logger.Session("listvolumes")
	logger.Info("start")
	defer logger.Info("end")

	logger.Debug(fmt.Sprintf("reference count: %#v", dw.volumes))

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
