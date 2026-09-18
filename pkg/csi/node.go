package csi

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"slices"

	"github.com/serverscom/rbs-csi-driver/pkg/iscsi"
	"github.com/serverscom/rbs-csi-driver/pkg/mount"
	"github.com/serverscom/rbs-csi-driver/pkg/util"
)

// NodeService implements the CSI Node service
type NodeService struct {
	csi.UnimplementedNodeServer
	nodeID            string
	iscsiManager      iscsi.ISCSIManager
	mountManager      mount.MountManager
	stagingMu         sync.Mutex
	stagingOperations map[string]struct{}
}

// NewNodeService creates a new node service
func NewNodeService(nodeID string) *NodeService {
	return &NodeService{
		nodeID:            nodeID,
		iscsiManager:      iscsi.NewManager(),
		mountManager:      mount.NewManager(),
		stagingOperations: make(map[string]struct{}),
	}
}

// tryBeginStage records an active stage operation. Kubelet can retry while a
// format is still running; retaining each retry until it gets the mutex grows
// the node driver's memory use. Aborted makes kubelet retry after completion.
func (s *NodeService) tryBeginStage(volumeID string) bool {
	s.stagingMu.Lock()
	defer s.stagingMu.Unlock()

	if s.stagingOperations == nil {
		s.stagingOperations = make(map[string]struct{})
	}
	if _, exists := s.stagingOperations[volumeID]; exists {
		return false
	}
	s.stagingOperations[volumeID] = struct{}{}
	return true
}

func (s *NodeService) endStage(volumeID string) {
	s.stagingMu.Lock()
	defer s.stagingMu.Unlock()
	delete(s.stagingOperations, volumeID)
}

// NodeStageVolume stages a volume to the staging path
func (s *NodeService) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	klog.V(2).InfoS("NodeStageVolume called", "volume_id", req.GetVolumeId())

	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if !s.tryBeginStage(volumeID) {
		return nil, status.Errorf(codes.Aborted, "stage operation is already in progress for volume %s", volumeID)
	}
	defer s.endStage(volumeID)

	// Extract iSCSI connection information from publish context
	publishContext := req.GetPublishContext()
	klog.V(2).InfoS("PublishContext", "context", util.MaskSensitiveMap(publishContext))

	targetIQN, ok := publishContext["target-iqn"]
	if !ok {
		klog.Error("target-iqn not found in publish context")
		return nil, status.Errorf(codes.InvalidArgument, "target-iqn not found in publish context")
	}

	ipAddress, ok := publishContext["ip-address"]
	if !ok {
		klog.Error("ip-address not found in publish context")
		return nil, status.Errorf(codes.InvalidArgument, "ip-address not found in publish context")
	}

	username, ok := publishContext["username"]
	if !ok {
		klog.Error("username not found in publish context")
		return nil, status.Errorf(codes.InvalidArgument, "username not found in publish context")
	}

	password, ok := publishContext["password"]
	if !ok {
		klog.Error("password not found in publish context")
		return nil, status.Errorf(codes.InvalidArgument, "password not found in publish context")
	}

	// Create staging directory
	stagingPath := req.GetStagingTargetPath()
	klog.V(1).InfoS("Creating staging directory", "staging_path", stagingPath)
	if err := os.MkdirAll(stagingPath, 0755); err != nil {
		klog.ErrorS(err, "Failed to create staging directory")
		return nil, status.Errorf(codes.Internal, "failed to create staging directory: %v", err)
	}

	// Prepare iSCSI target info
	portal := fmt.Sprintf("%s:3260", ipAddress) // Default iSCSI port
	target := &iscsi.TargetInfo{
		Portal:   portal,
		IQN:      targetIQN,
		Username: username,
		Password: password,
	}
	klog.V(2).InfoS("Prepared iSCSI target", "portal", portal, "iqn", targetIQN)

	if err := SaveTargetInfo(stagingPath, target); err != nil {
		klog.ErrorS(err, "Failed to save target info")
		return nil, status.Errorf(codes.Internal, "failed to save target info: %v", err)
	}

	// Check if already logged in
	klog.V(2).InfoS("Checking iscsi login status")
	loggedIn, err := s.iscsiManager.IsLoggedIn(ctx, target)
	if err != nil {
		klog.ErrorS(err, "Failed to check login status")
		return nil, status.Errorf(codes.Internal, "failed to check login status: %v", err)
	}
	klog.V(1).InfoS("iscsi login status", "logged_in", loggedIn)

	var devicePath string
	if !loggedIn {
		klog.V(1).InfoS("Not logged in, starting iscsi discovery and login")
		// Discover targets
		klog.V(2).InfoS("Discovering targets on portal", "portal", portal)
		targets, err := s.iscsiManager.DiscoverTargets(ctx, portal)
		if err != nil {
			klog.ErrorS(err, "Failed to discover targets")
			return nil, status.Errorf(codes.Internal, "failed to discover targets: %v", err)
		}
		klog.V(2).InfoS("Discovered targets", "targets_count", len(targets), "targets", targets)

		found := slices.Contains(targets, targetIQN)

		if !found {
			return nil, status.Errorf(codes.Internal, "target %s not found on portal %s", targetIQN, portal)
		}

		// Login to target
		if err := s.iscsiManager.Login(ctx, target); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to login to target: %v", err)
		}
	}

	// Get device path
	devicePath, err = s.iscsiManager.GetDevice(ctx, target)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get device path: %v", err)
	}

	// Wait for device to be ready
	if err := s.iscsiManager.WaitForDevice(ctx, devicePath, 30*time.Second); err != nil {
		return nil, status.Errorf(codes.Internal, "device not ready: %v", err)
	}

	// Get filesystem type
	fsType := "ext4" // Default filesystem type
	if cap := req.GetVolumeCapability(); cap != nil {
		if mount := cap.GetMount(); mount != nil && mount.GetFsType() != "" {
			fsType = mount.GetFsType()
		}
	}

	mounted, err := s.mountManager.IsMounted(stagingPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check if staging path is mounted: %v", err)
	}
	if mounted {
		klog.V(1).InfoS("Staging path already mounted, volume already staged",
			"volume_id", req.GetVolumeId(), "staging_path", stagingPath)
		return &csi.NodeStageVolumeResponse{}, nil
	}

	// Use FormatAndMount which will check if already formatted and only format if needed
	klog.V(2).InfoS("Formatting and mounting device if needed",
		"device_path", devicePath,
		"staging_path", stagingPath,
		"fs_type", fsType)

	if err := s.mountManager.FormatAndMountDevice(ctx, devicePath, stagingPath, fsType, nil); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to format and mount device: %v", err)
	}

	klog.InfoS("Volume staged successfully", "volume_id", req.GetVolumeId())
	return &csi.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume unstages a volume from the staging path
func (s *NodeService) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	klog.V(2).InfoS("NodeUnstageVolume called", "volume_id", req.GetVolumeId())

	stagingPath := req.GetStagingTargetPath()

	mounted, err := s.mountManager.IsMounted(stagingPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check mount: %v", err)
	}

	if mounted {
		klog.V(2).InfoS("Unmounting staging path", "staging_path", stagingPath)
		if err := s.mountManager.Unmount(ctx, stagingPath); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to unmount staging path: %v", err)
		}
	}

	// Load target info and cleanup iSCSI
	target, err := LoadTargetInfo(stagingPath)
	if err == nil {
		if err := s.iscsiManager.CleanupTarget(ctx, target); err != nil {
			return nil, status.Errorf(codes.Internal, "iscsi cleanup failed: %v", err)
		}
	}

	_ = DeleteTargetInfo(stagingPath)

	if err := os.RemoveAll(stagingPath); err != nil {
		klog.ErrorS(err, "Failed to remove staging directory", "path", stagingPath)
	}

	klog.V(1).InfoS("Volume unstaged successfully", "volume_id", req.GetVolumeId())
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume publishes a volume to the target path
func (s *NodeService) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	klog.V(2).InfoS("NodePublishVolume called", "volume_id", req.GetVolumeId())

	targetPath := req.GetTargetPath()
	stagingPath := req.GetStagingTargetPath()

	// Create target directory
	if err := os.MkdirAll(targetPath, 0755); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create target directory: %v", err)
	}

	// Check if target path is already mounted
	mounted, err := s.mountManager.IsMounted(targetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check if target path is mounted: %v", err)
	}

	if mounted {
		klog.V(1).InfoS("Target path is already mounted", "target_path", targetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	// Determine mount options
	options := []string{}
	if req.GetReadonly() {
		options = append(options, "ro")
	}

	// For filesystem mounts, bind mount from staging path
	klog.V(1).InfoS("Bind mounting from staging path to target path",
		"staging_path", stagingPath,
		"target_path", targetPath)

	if err := s.mountManager.BindMount(ctx, stagingPath, targetPath, options); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to bind mount: %v", err)
	}

	klog.InfoS("Volume published successfully", "volume_id", req.GetVolumeId())
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unpublishes a volume from the target path
func (s *NodeService) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	klog.V(2).InfoS("NodeUnpublishVolume called", "volume_id", req.GetVolumeId())

	targetPath := req.GetTargetPath()

	// Check if target path is mounted
	mounted, err := s.mountManager.IsMounted(targetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check if target path is mounted: %v", err)
	}

	if mounted {
		// Unmount the target path
		klog.V(1).InfoS("Unmounting target path", "target_path", targetPath)
		if err := s.mountManager.Unmount(ctx, targetPath); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to unmount target path: %v", err)
		}
	}

	// Remove target directory if it's empty
	if empty, err := mount.IsDirectoryEmpty(targetPath); err == nil && empty {
		if err := os.Remove(targetPath); err != nil {
			klog.ErrorS(err, "Failed to remove target directory")
		}
	}

	klog.InfoS("Volume unpublished successfully", "volume_id", req.GetVolumeId())
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeExpandVolume expands a volume on the node
func (s *NodeService) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	klog.V(2).InfoS("NodeExpandVolume called", "volume_id", req.GetVolumeId())

	volumePath := req.GetVolumePath()

	// Get mount info to determine device and filesystem type
	mountInfo, err := s.mountManager.GetMountInfo(volumePath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get mount info: %v", err)
	}

	klog.V(2).InfoS("Rescanning iscsi device to detect new size", "device", mountInfo.Device)
	// Rescan the iSCSI device to detect the new size
	if err := s.iscsiManager.RescanDevice(ctx, mountInfo.Device); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to rescan device: %v", err)
	}

	// Resize filesystem
	fsType := mountInfo.FSType
	klog.V(2).InfoS("Resizing filesystem on device", "fs_type", fsType, "device", mountInfo.Device)
	switch fsType {
	case "ext4", "ext3":
		// For ext filesystems, resize using the device
		if err := s.mountManager.ResizeFilesystem(ctx, mountInfo.Device, fsType); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to resize filesystem: %v", err)
		}
	case "xfs":
		// For XFS, resize using the mount path
		if err := s.mountManager.ResizeFilesystemAtPath(ctx, volumePath, fsType); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to resize filesystem: %v", err)
		}
	default:
		return nil, status.Errorf(codes.Internal, "unsupported filesystem type for resize: %s", fsType)
	}

	response := &csi.NodeExpandVolumeResponse{
		CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
	}

	klog.InfoS("Volume expanded successfully", "volume_id", req.GetVolumeId())
	return response, nil
}

// GetNodeID returns the node ID
func (s *NodeService) GetNodeID() string {
	return s.nodeID
}

// NodeGetCapabilities returns the capabilities of the node
func (s *NodeService) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	klog.V(2).InfoS("NodeGetCapabilities called")

	capabilities := []*csi.NodeServiceCapability{
		{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
				},
			},
		},
		{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
				},
			},
		},
	}

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: capabilities,
	}, nil
}

// NodeGetInfo returns node information
func (s *NodeService) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	klog.V(2).InfoS("NodeGetInfo called")

	return &csi.NodeGetInfoResponse{
		NodeId: s.nodeID,
		// MaxVolumesPerNode can be set if there's a limit
		// AccessibleTopology can be set for topology awareness
	}, nil
}
