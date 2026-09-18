package csi

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	. "github.com/onsi/gomega"
	"github.com/serverscom/rbs-csi-driver/pkg/iscsi"
	"github.com/serverscom/rbs-csi-driver/pkg/mocks"
	"github.com/serverscom/rbs-csi-driver/pkg/mount"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newTestNode(ctrl *gomock.Controller) (*NodeService, *mocks.MockISCSIManager, *mocks.MockMountManager) {
	miscsi := mocks.NewMockISCSIManager(ctrl)
	mmount := mocks.NewMockMountManager(ctrl)
	return &NodeService{
		nodeID:       "node-test",
		iscsiManager: miscsi,
		mountManager: mmount,
	}, miscsi, mmount
}

func TestNodeService_StageOperationIsReleased(t *testing.T) {
	g := NewGomegaWithT(t)
	svc := &NodeService{}

	g.Expect(svc.tryBeginStage("vol-1")).To(BeTrue())
	g.Expect(svc.tryBeginStage("vol-1")).To(BeFalse())
	svc.endStage("vol-1")
	g.Expect(svc.tryBeginStage("vol-1")).To(BeTrue())
}

func TestNodeStageVolume_Success(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, miscsi, mmount := newTestNode(ctrl)
	ctx := context.Background()

	req := &csi.NodeStageVolumeRequest{
		VolumeId:          "vol-1",
		StagingTargetPath: "/tmp/staging",
		PublishContext: map[string]string{
			"target-iqn": "iqn.2024-01.com.example:target",
			"ip-address": "192.168.1.100",
			"username":   "testuser",
			"password":   "testpass",
		},
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "ext4",
				},
			},
		},
	}

	target := &iscsi.TargetInfo{
		Portal:   "192.168.1.100:3260",
		IQN:      "iqn.2024-01.com.example:target",
		Username: "testuser",
		Password: "testpass",
	}

	miscsi.EXPECT().IsLoggedIn(ctx, target).Return(false, nil)
	miscsi.EXPECT().DiscoverTargets(ctx, "192.168.1.100:3260").
		Return([]string{"iqn.2024-01.com.example:target"}, nil)
	miscsi.EXPECT().Login(ctx, target).Return(nil)
	miscsi.EXPECT().GetDevice(ctx, target).Return("/dev/sdb", nil)
	miscsi.EXPECT().WaitForDevice(ctx, "/dev/sdb", 30*time.Second).Return(nil)
	mmount.EXPECT().IsMounted("/tmp/staging").Return(false, nil)
	mmount.EXPECT().FormatAndMountDevice(ctx, "/dev/sdb", "/tmp/staging", "ext4", nil).Return(nil)

	resp, err := svc.NodeStageVolume(ctx, req)

	g.Expect(err).To(BeNil())
	g.Expect(resp).NotTo(BeNil())
}

func TestNodeStageVolume_AlreadyLoggedIn(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, miscsi, mmount := newTestNode(ctrl)
	ctx := context.Background()

	req := &csi.NodeStageVolumeRequest{
		VolumeId:          "vol-1",
		StagingTargetPath: "/tmp/staging",
		PublishContext: map[string]string{
			"target-iqn": "iqn.2024-01.com.example:target",
			"ip-address": "192.168.1.100",
			"username":   "testuser",
			"password":   "testpass",
		},
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "xfs",
				},
			},
		},
	}

	target := &iscsi.TargetInfo{
		Portal:   "192.168.1.100:3260",
		IQN:      "iqn.2024-01.com.example:target",
		Username: "testuser",
		Password: "testpass",
	}

	miscsi.EXPECT().IsLoggedIn(ctx, target).Return(true, nil)
	miscsi.EXPECT().GetDevice(ctx, target).Return("/dev/sdb", nil)
	miscsi.EXPECT().WaitForDevice(ctx, "/dev/sdb", 30*time.Second).Return(nil)
	mmount.EXPECT().IsMounted("/tmp/staging").Return(false, nil)
	mmount.EXPECT().FormatAndMountDevice(ctx, "/dev/sdb", "/tmp/staging", "xfs", nil).Return(nil)

	resp, err := svc.NodeStageVolume(ctx, req)

	g.Expect(err).To(BeNil())
	g.Expect(resp).NotTo(BeNil())
}

func TestNodeStageVolume_AlreadyStaged(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, miscsi, mmount := newTestNode(ctrl)
	ctx := context.Background()

	req := &csi.NodeStageVolumeRequest{
		VolumeId:          "vol-1",
		StagingTargetPath: "/tmp/staging",
		PublishContext: map[string]string{
			"target-iqn": "iqn.2024-01.com.example:target",
			"ip-address": "192.168.1.100",
			"username":   "testuser",
			"password":   "testpass",
		},
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "ext4",
				},
			},
		},
	}

	target := &iscsi.TargetInfo{
		Portal:   "192.168.1.100:3260",
		IQN:      "iqn.2024-01.com.example:target",
		Username: "testuser",
		Password: "testpass",
	}

	miscsi.EXPECT().IsLoggedIn(ctx, target).Return(true, nil)
	miscsi.EXPECT().GetDevice(ctx, target).Return("/dev/sdb", nil)
	miscsi.EXPECT().WaitForDevice(ctx, "/dev/sdb", 30*time.Second).Return(nil)
	mmount.EXPECT().IsMounted("/tmp/staging").Return(true, nil)
	// FormatAndMountDevice must NOT be called for an already-staged volume

	resp, err := svc.NodeStageVolume(ctx, req)

	g.Expect(err).To(BeNil())
	g.Expect(resp).NotTo(BeNil())
}

func TestNodeStageVolume_MissingTargetIQN(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, _, _ := newTestNode(ctrl)
	ctx := context.Background()

	req := &csi.NodeStageVolumeRequest{
		VolumeId:          "vol-1",
		StagingTargetPath: "/tmp/staging",
		PublishContext: map[string]string{
			"ip-address": "192.168.1.100",
			"username":   "testuser",
			"password":   "testpass",
		},
	}

	resp, err := svc.NodeStageVolume(ctx, req)

	g.Expect(err).NotTo(BeNil())
	g.Expect(resp).To(BeNil())
	g.Expect(status.Code(err)).To(Equal(codes.InvalidArgument))
}

func TestNodeStageVolume_DiscoverTargetsFails(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, miscsi, _ := newTestNode(ctrl)
	ctx := context.Background()

	req := &csi.NodeStageVolumeRequest{
		VolumeId:          "vol-1",
		StagingTargetPath: "/tmp/staging",
		PublishContext: map[string]string{
			"target-iqn": "iqn.2024-01.com.example:target",
			"ip-address": "192.168.1.100",
			"username":   "testuser",
			"password":   "testpass",
		},
	}

	target := &iscsi.TargetInfo{
		Portal:   "192.168.1.100:3260",
		IQN:      "iqn.2024-01.com.example:target",
		Username: "testuser",
		Password: "testpass",
	}

	miscsi.EXPECT().IsLoggedIn(ctx, target).Return(false, nil)
	miscsi.EXPECT().DiscoverTargets(ctx, "192.168.1.100:3260").
		Return(nil, errors.New("discovery failed"))

	resp, err := svc.NodeStageVolume(ctx, req)

	g.Expect(err).NotTo(BeNil())
	g.Expect(resp).To(BeNil())
	g.Expect(status.Code(err)).To(Equal(codes.Internal))
}

func TestNodeStageVolume_TargetNotFound(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, miscsi, _ := newTestNode(ctrl)
	ctx := context.Background()

	req := &csi.NodeStageVolumeRequest{
		VolumeId:          "vol-1",
		StagingTargetPath: "/tmp/staging",
		PublishContext: map[string]string{
			"target-iqn": "iqn.2024-01.com.example:target",
			"ip-address": "192.168.1.100",
			"username":   "testuser",
			"password":   "testpass",
		},
	}

	target := &iscsi.TargetInfo{
		Portal:   "192.168.1.100:3260",
		IQN:      "iqn.2024-01.com.example:target",
		Username: "testuser",
		Password: "testpass",
	}

	miscsi.EXPECT().IsLoggedIn(ctx, target).Return(false, nil)
	miscsi.EXPECT().DiscoverTargets(ctx, "192.168.1.100:3260").
		Return([]string{"iqn.2024-01.com.example:other"}, nil)

	resp, err := svc.NodeStageVolume(ctx, req)

	g.Expect(err).NotTo(BeNil())
	g.Expect(resp).To(BeNil())
	g.Expect(status.Code(err)).To(Equal(codes.Internal))
}

func TestNodeUnstageVolume_Success(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, iscsiMgr, mmount := newTestNode(ctrl)
	ctx := context.Background()

	req := &csi.NodeUnstageVolumeRequest{
		VolumeId:          "vol-1",
		StagingTargetPath: "/tmp/staging",
	}

	mmount.EXPECT().IsMounted("/tmp/staging").Return(true, nil)
	mmount.EXPECT().Unmount(ctx, "/tmp/staging").Return(nil)

	target := &iscsi.TargetInfo{
		IQN:    "iqn.test",
		Portal: "127.0.0.1:3260",
	}
	data, _ := json.Marshal(target)
	_ = os.MkdirAll(req.StagingTargetPath, 0755)
	_ = os.WriteFile(filepath.Join(req.StagingTargetPath, ".target-info"), data, 0600)

	iscsiMgr.EXPECT().CleanupTarget(ctx, target).Return(nil)

	resp, err := svc.NodeUnstageVolume(ctx, req)

	g.Expect(err).To(BeNil())
	g.Expect(resp).NotTo(BeNil())

	_ = os.RemoveAll(req.StagingTargetPath)
}

func TestNodeUnstageVolume_NotMounted(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, iscsiMgr, mmount := newTestNode(ctrl)
	ctx := context.Background()

	req := &csi.NodeUnstageVolumeRequest{
		VolumeId:          "vol-1",
		StagingTargetPath: "/tmp/staging",
	}

	mmount.EXPECT().IsMounted("/tmp/staging").Return(false, nil)

	target := &iscsi.TargetInfo{
		IQN:    "iqn.test",
		Portal: "127.0.0.1:3260",
	}
	_ = os.MkdirAll(req.StagingTargetPath, 0755)
	_ = os.WriteFile(filepath.Join(req.StagingTargetPath, ".target-info"), []byte(`{"IQN":"iqn.test","Portal":"127.0.0.1:3260"}`), 0600)

	iscsiMgr.EXPECT().CleanupTarget(ctx, target).Return(nil)

	resp, err := svc.NodeUnstageVolume(ctx, req)

	g.Expect(err).To(BeNil())
	g.Expect(resp).NotTo(BeNil())

	_ = os.RemoveAll(req.StagingTargetPath)
}

func TestNodePublishVolume_Success(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, _, mmount := newTestNode(ctrl)
	ctx := context.Background()

	req := &csi.NodePublishVolumeRequest{
		VolumeId:          "vol-1",
		TargetPath:        "/tmp/target",
		StagingTargetPath: "/tmp/staging",
		Readonly:          false,
	}

	mmount.EXPECT().IsMounted("/tmp/target").Return(false, nil)
	mmount.EXPECT().BindMount(ctx, "/tmp/staging", "/tmp/target", []string{}).Return(nil)

	resp, err := svc.NodePublishVolume(ctx, req)

	g.Expect(err).To(BeNil())
	g.Expect(resp).NotTo(BeNil())
}

func TestNodePublishVolume_ReadOnly(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, _, mmount := newTestNode(ctrl)
	ctx := context.Background()

	req := &csi.NodePublishVolumeRequest{
		VolumeId:          "vol-1",
		TargetPath:        "/tmp/target",
		StagingTargetPath: "/tmp/staging",
		Readonly:          true,
	}

	mmount.EXPECT().IsMounted("/tmp/target").Return(false, nil)
	mmount.EXPECT().BindMount(ctx, "/tmp/staging", "/tmp/target", []string{"ro"}).Return(nil)

	resp, err := svc.NodePublishVolume(ctx, req)

	g.Expect(err).To(BeNil())
	g.Expect(resp).NotTo(BeNil())
}

func TestNodePublishVolume_AlreadyMounted(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, _, mmount := newTestNode(ctrl)
	ctx := context.Background()

	req := &csi.NodePublishVolumeRequest{
		VolumeId:          "vol-1",
		TargetPath:        "/tmp/target",
		StagingTargetPath: "/tmp/staging",
	}

	mmount.EXPECT().IsMounted("/tmp/target").Return(true, nil)

	resp, err := svc.NodePublishVolume(ctx, req)

	g.Expect(err).To(BeNil())
	g.Expect(resp).NotTo(BeNil())
}

func TestNodePublishVolume_BindMountFails(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, _, mmount := newTestNode(ctrl)
	ctx := context.Background()

	req := &csi.NodePublishVolumeRequest{
		VolumeId:          "vol-1",
		TargetPath:        "/tmp/target",
		StagingTargetPath: "/tmp/staging",
	}

	mmount.EXPECT().IsMounted("/tmp/target").Return(false, nil)
	mmount.EXPECT().BindMount(ctx, "/tmp/staging", "/tmp/target", []string{}).
		Return(errors.New("already mounted"))

	resp, err := svc.NodePublishVolume(ctx, req)

	g.Expect(err).NotTo(BeNil())
	g.Expect(resp).To(BeNil())
	g.Expect(status.Code(err)).To(Equal(codes.Internal))
}

func TestNodeUnpublishVolume_Success(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, _, mmount := newTestNode(ctrl)
	ctx := context.Background()

	req := &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "vol-1",
		TargetPath: "/tmp/target",
	}

	mmount.EXPECT().IsMounted("/tmp/target").Return(true, nil)
	mmount.EXPECT().Unmount(ctx, "/tmp/target").Return(nil)

	resp, err := svc.NodeUnpublishVolume(ctx, req)

	g.Expect(err).To(BeNil())
	g.Expect(resp).NotTo(BeNil())
}

func TestNodeUnpublishVolume_NotMounted(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, _, mmount := newTestNode(ctrl)
	ctx := context.Background()

	req := &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "vol-1",
		TargetPath: "/tmp/target",
	}

	mmount.EXPECT().IsMounted("/tmp/target").Return(false, nil)

	resp, err := svc.NodeUnpublishVolume(ctx, req)

	g.Expect(err).To(BeNil())
	g.Expect(resp).NotTo(BeNil())
}

func TestNodeExpandVolume_Ext4Success(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, miscsi, mmount := newTestNode(ctrl)
	ctx := context.Background()

	req := &csi.NodeExpandVolumeRequest{
		VolumeId:   "vol-1",
		VolumePath: "/tmp/target",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 20 * 1024 * 1024 * 1024,
		},
	}

	mmount.EXPECT().GetMountInfo("/tmp/target").Return(&mount.MountInfo{
		Device: "/dev/sdb",
		FSType: "ext4",
	}, nil)
	miscsi.EXPECT().RescanDevice(ctx, "/dev/sdb").Return(nil)
	mmount.EXPECT().ResizeFilesystem(ctx, "/dev/sdb", "ext4").Return(nil)

	resp, err := svc.NodeExpandVolume(ctx, req)

	g.Expect(err).To(BeNil())
	g.Expect(resp).NotTo(BeNil())
	g.Expect(resp.CapacityBytes).To(Equal(int64(20 * 1024 * 1024 * 1024)))
}

func TestNodeExpandVolume_UnsupportedFilesystem(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, miscsi, mmount := newTestNode(ctrl)
	ctx := context.Background()

	req := &csi.NodeExpandVolumeRequest{
		VolumeId:   "vol-1",
		VolumePath: "/tmp/target",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 20 * 1024 * 1024 * 1024,
		},
	}

	mmount.EXPECT().GetMountInfo("/tmp/target").Return(&mount.MountInfo{
		Device: "/dev/sdb",
		FSType: "btrfs",
	}, nil)
	miscsi.EXPECT().RescanDevice(ctx, "/dev/sdb").Return(nil)

	resp, err := svc.NodeExpandVolume(ctx, req)

	g.Expect(err).NotTo(BeNil())
	g.Expect(resp).To(BeNil())
	g.Expect(status.Code(err)).To(Equal(codes.Internal))
}

func TestNodeGetCapabilities(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, _, _ := newTestNode(ctrl)
	ctx := context.Background()

	resp, err := svc.NodeGetCapabilities(ctx, &csi.NodeGetCapabilitiesRequest{})

	g.Expect(err).To(BeNil())
	g.Expect(resp).NotTo(BeNil())
	g.Expect(resp.Capabilities).To(HaveLen(2))

	capTypes := make([]csi.NodeServiceCapability_RPC_Type, 0, 2)
	for _, cap := range resp.Capabilities {
		capTypes = append(capTypes, cap.GetRpc().GetType())
	}

	g.Expect(capTypes).To(ContainElement(csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME))
	g.Expect(capTypes).To(ContainElement(csi.NodeServiceCapability_RPC_EXPAND_VOLUME))
}

func TestNodeGetInfo(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, _, _ := newTestNode(ctrl)
	ctx := context.Background()

	resp, err := svc.NodeGetInfo(ctx, &csi.NodeGetInfoRequest{})

	g.Expect(err).To(BeNil())
	g.Expect(resp).NotTo(BeNil())
	g.Expect(resp.NodeId).To(Equal("node-test"))
}

func TestGetNodeID(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, _, _ := newTestNode(ctrl)

	nodeID := svc.GetNodeID()

	g.Expect(nodeID).To(Equal("node-test"))
}
