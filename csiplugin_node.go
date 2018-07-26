package csiplugin

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"reflect"
	"sync"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"code.cloudfoundry.org/csishim"
	"code.cloudfoundry.org/goshims/execshim"
	"code.cloudfoundry.org/goshims/grpcshim"
	"code.cloudfoundry.org/goshims/osshim"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/voldriver/driverhttp"
	"code.cloudfoundry.org/voldriver/invoker"
	"code.cloudfoundry.org/volman"
	"github.com/container-storage-interface/spec/lib/go/csi/v0"
)

const MAPFS_MOUNT_TIMEOUT = time.Minute * 5

type OsHelper interface {
	Umask(mask int) (oldmask int)
}

type volumeInfo struct {
	csiVolumeId  string
	count        int
	mapfsMounted bool
}

type nodeWrapper struct {
	Impl            interface{}
	Spec            volman.PluginSpec
	grpcShim        grpcshim.Grpc
	csiShim         csishim.Csi
	osShim          osshim.Os
	volumes         map[string]*volumeInfo
	volumesMutex    sync.RWMutex
	csiMountRootDir string
	invoker         invoker.Invoker
	bgInvoker       BackgroundInvoker
	osHelper        OsHelper
	mapfsPath       string
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
	nodePlugin := dw.csiShim.NewNodeClient(conn)
	var csiVolumeId string
	dw.volumesMutex.Lock()
	defer dw.volumesMutex.Unlock()

	mountPath := path.Join(dw.csiMountRootDir, "mounts", dw.Spec.Name)
	tmpPath := path.Join(dw.csiMountRootDir, "tmp", dw.Spec.Name)

	var targetPath string

	if volInfo, ok := dw.volumes[volumeId]; ok {
		csiVolumeId = volInfo.csiVolumeId
		count := volInfo.count
		if count > 1 {
			volInfo.count = volInfo.count - 1
		} else {

			if volInfo.mapfsMounted {
				targetPath = path.Join(tmpPath, volumeId)
				ctx := context.TODO()
				env := driverhttp.NewHttpDriverEnv(logger, ctx)
				_, err := dw.invoker.Invoke(env, "fusermount", []string{"-u", path.Join(mountPath, volumeId)})
				if err != nil {
					logger.Error("invoke-unmount-failed", err)
					return err
				}
			} else {
				targetPath = path.Join(mountPath, volumeId)
			}
			nodeResponse, err := nodePlugin.NodeUnpublishVolume(context.TODO(), &csi.NodeUnpublishVolumeRequest{
				VolumeId:   csiVolumeId,
				TargetPath: targetPath,
			})
			logger.Debug("node-response", lager.Data{"nodeResponse": nodeResponse})
			if err != nil {
				logger.Error("node-response-error", err)
				return err
			}
			delete(dw.volumes, volumeId)
		}
	} else {
		return errors.New(fmt.Sprintf("unknown volumeId: %s", volumeId))
	}

	return nil
}

func (dw *nodeWrapper) Mount(logger lager.Logger, volumeId string, config map[string]interface{}) (volman.MountResponse, error) {
	logger = logger.Session("mount")
	logger.Info("start")
	defer logger.Info("end")
	logger.Debug("mount-volume", lager.Data{"config": config})

	conn, err := dw.grpcShim.Dial(dw.Spec.Address, grpc.WithInsecure())
	defer conn.Close()
	if err != nil {
		logger.Error("grpc-dial", err, lager.Data{"address": dw.Spec.Address})
		return volman.MountResponse{}, err
	}

	mountPath := path.Join(dw.csiMountRootDir, "mounts", dw.Spec.Name)
	tmpPath := path.Join(dw.csiMountRootDir, "tmp", dw.Spec.Name)

	nodePlugin := dw.csiShim.NewNodeClient(conn)
	publishRequestVolID, ok := config["id"].(string)
	if !ok {
		err := errors.New(fmt.Sprintf("type assertion on VolumeId: not string, but %T", config["id"]))
		logger.Error("bind-config", err)
		return volman.MountResponse{}, err
	}

	var volAttrs map[string]string
	if config["attributes"] != nil {

		attributesJSON, err := json.Marshal(config["attributes"])
		if err != nil {
			logger.Error("marshal-attributes", err, lager.Data{"attributes": config["attributes"]})
			return volman.MountResponse{}, err
		}

		err = json.Unmarshal(attributesJSON, &volAttrs)
		if err != nil {
			logger.Error("unmarshal-attributes", err, lager.Data{"attributes": config["attributes"]})
			return volman.MountResponse{}, err
		}
	}

	var targetPath string
	if config["binding-params"] != nil {
		targetPath = path.Join(tmpPath, volumeId)
	} else {
		targetPath = path.Join(mountPath, volumeId)
	}

	err = dw.createDirifNotExist(targetPath)
	if err != nil {
		logger.Error("mkdir-path-failed", err)
		return volman.MountResponse{}, err
	}

	dw.volumesMutex.Lock()
	defer dw.volumesMutex.Unlock()

	var do_mount bool

	if volInfo, ok := dw.volumes[volumeId]; ok {
		if volInfo.count < 1 {
			do_mount = true
		} else {
			do_mount = false
		}
	} else {
		do_mount = true
	}

	var nodeResponse *csi.NodePublishVolumeResponse
	var success bool

	if do_mount {
		nodeResponse, err = nodePlugin.NodePublishVolume(context.TODO(), &csi.NodePublishVolumeRequest{
			VolumeId:   publishRequestVolID,
			TargetPath: targetPath,
			VolumeCapability: &csi.VolumeCapability{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
				},
			},
			VolumeAttributes: volAttrs,
		})
		defer func() {
			if !success {
				nodeResponse, err := nodePlugin.NodeUnpublishVolume(context.TODO(), &csi.NodeUnpublishVolumeRequest{
					VolumeId:   publishRequestVolID,
					TargetPath: targetPath,
				})
				if err != nil {
					logger.Error("failed-to-cleanup-original-mount", err)
					return
				}
				logger.Debug("unmounted-original-volume", lager.Data{"nodeResponse": nodeResponse})
			}
		}()

	}
	var uid, gid string

	if config["binding-params"] != nil {
		ctx := context.TODO()
		env := driverhttp.NewHttpDriverEnv(logger, ctx)
		uid = config["binding-params"].(map[string]interface{})["uid"].(string)
		gid = config["binding-params"].(map[string]interface{})["gid"].(string)
		mountPoint := path.Join(mountPath, volumeId)
		original := path.Join(tmpPath, volumeId)

		err = dw.createDirifNotExist(mountPoint)
		if err != nil {
			logger.Error("mkdir-path-failed", err)
			return volman.MountResponse{}, err
		}

		if do_mount {
			err := dw.bgInvoker.Invoke(env, dw.mapfsPath, []string{"-uid", uid, "-gid", gid, mountPoint, original}, "Mounted!", MAPFS_MOUNT_TIMEOUT)
			if err != nil {
				logger.Error("background-invoke-mount-failed", err)
				return volman.MountResponse{}, err
			}
		}

		targetPath = mountPoint
	}

	logger.Debug("node-response", lager.Data{"nodeResponse": nodeResponse})

	if err != nil {
		logger.Error("node-response-error", err)
		return volman.MountResponse{}, err
	}

	if volInfo, ok := dw.volumes[volumeId]; ok {
		volInfo.csiVolumeId = publishRequestVolID
		volInfo.count = volInfo.count + 1
	} else {
		dw.volumes[volumeId] = &volumeInfo{
			csiVolumeId: publishRequestVolID,
			count:       1,
		}
	}

	if config["binding-params"] != nil {
		volInfo, _ := dw.volumes[volumeId]
		volInfo.mapfsMounted = true
	}

	success = true
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

func (dw *nodeWrapper) createDirifNotExist(path string) error {
	if _, err := dw.osShim.Stat(path); os.IsNotExist(err) {
		orig := dw.osHelper.Umask(000)
		defer dw.osHelper.Umask(orig)
		err := dw.osShim.MkdirAll(path, os.ModePerm)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewCsiPlugin(plugin csi.NodeClient, pluginSpec volman.PluginSpec, grpcShim grpcshim.Grpc, csiShim csishim.Csi, osShim osshim.Os, csiMountRootDir string, oshelper OsHelper, mapfsPath string) volman.Plugin {
	return &nodeWrapper{
		Impl:            plugin,
		Spec:            pluginSpec,
		grpcShim:        grpcShim,
		csiShim:         csiShim,
		osShim:          osShim,
		volumes:         map[string]*volumeInfo{},
		csiMountRootDir: csiMountRootDir,
		invoker:         invoker.NewRealInvoker(),
		bgInvoker:       NewBackgroundInvoker(&execshim.ExecShim{}),
		osHelper:        oshelper,
		mapfsPath:       mapfsPath,
	}
}

func NewCsiPluginWithInvoker(invoker invoker.Invoker, bgInvoker BackgroundInvoker, plugin csi.NodeClient, pluginSpec volman.PluginSpec, grpcShim grpcshim.Grpc, csiShim csishim.Csi, osShim osshim.Os, csiMountRootDir string, oshelper OsHelper, mapfsPath string) volman.Plugin {
	return &nodeWrapper{
		Impl:            plugin,
		Spec:            pluginSpec,
		grpcShim:        grpcShim,
		csiShim:         csiShim,
		osShim:          osShim,
		volumes:         map[string]*volumeInfo{},
		csiMountRootDir: csiMountRootDir,
		invoker:         invoker,
		bgInvoker:       bgInvoker,
		osHelper:        oshelper,
		mapfsPath:       mapfsPath,
	}
}
