package csiplugin

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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

type nodeWrapper struct {
	Impl            interface{}
	Spec            volman.PluginSpec
	grpcShim        grpcshim.Grpc
	csiShim         csishim.Csi
	osShim          osshim.Os
	csiMountRootDir string
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

	err = createVolumesRootifNotExist(logger, dw.csiMountRootDir, dw.osShim)
	if err != nil {
		logger.Error("create-volumes-root", err)
		return volman.MountResponse{}, err
	}

	// Diego executor can only bind mount path starting with /var/vcap/data?
	targetPath := dw.csiMountRootDir + "/" + volumeId
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

func createVolumesRootifNotExist(logger lager.Logger, volumesRoot string, osShim osshim.Os) error {
	dir, err := filepath.Abs(volumesRoot)
	if err != nil {
		logger.Fatal("abs-failed", err)
	}

	if !strings.HasSuffix(dir, "/") {
		dir = fmt.Sprintf("%s/", dir)
	}

	mountsPathRoot := fmt.Sprintf("%s%s", dir, volumesRoot)
	logger.Debug(mountsPathRoot)
	_, err = osShim.Stat(mountsPathRoot)

	if err != nil {
		if osShim.IsNotExist(err) {
			// Create the directory if not exist
			orig := syscall.Umask(000)
			defer syscall.Umask(orig)
			err = osShim.MkdirAll(mountsPathRoot, os.ModePerm)
			if err != nil {
				logger.Error("mkdirall", err)
				return err
			}
		}
	}
	return nil
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
		csiMountRootDir: csiMountRootDir,
	}
}
