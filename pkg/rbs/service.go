package rbs

import (
	"context"
	"fmt"

	serverscom "github.com/serverscom/serverscom-go-client/pkg"
	"k8s.io/klog/v2"
)

//go:generate mockgen --destination ../mocks/rbs_service.go --package=mocks --source service.go
type RBSService interface {
	// rbs
	CreateVolume(ctx context.Context, req serverscom.RemoteBlockStorageVolumeCreateInput) (*serverscom.RemoteBlockStorageVolume, error)
	GetVolume(ctx context.Context, volumeID string) (*serverscom.RemoteBlockStorageVolume, error)
	UpdateVolume(ctx context.Context, volumeID string, req serverscom.RemoteBlockStorageVolumeUpdateInput) (*serverscom.RemoteBlockStorageVolume, error)
	DeleteVolume(ctx context.Context, volumeID string) error
	GetVolumeCredentials(ctx context.Context, volumeID string) (*serverscom.RemoteBlockStorageVolumeCredentials, error)
	ResetVolumeCredentials(ctx context.Context, volumeID string) (*serverscom.RemoteBlockStorageVolume, error)

	// helpers
	FindVolumeByLabel(ctx context.Context, labelKey, labelValue string) (*serverscom.RemoteBlockStorageVolume, error)

	FindLocationByName(ctx context.Context, name string) (*serverscom.Location, error)
	FindFlavorByName(ctx context.Context, locationID int, name string) (*serverscom.RemoteBlockStorageFlavor, error)
}

type rbsService struct {
	client *serverscom.Client
}

// NewRBSService creates a new RBS service
func NewRBSService(client *serverscom.Client) RBSService {
	return &rbsService{
		client: client,
	}
}

func (s *rbsService) CreateVolume(ctx context.Context, req serverscom.RemoteBlockStorageVolumeCreateInput) (*serverscom.RemoteBlockStorageVolume, error) {
	return s.client.RemoteBlockStorageVolumes.Create(ctx, req)
}

func (s *rbsService) GetVolume(ctx context.Context, id string) (*serverscom.RemoteBlockStorageVolume, error) {
	return s.client.RemoteBlockStorageVolumes.Get(ctx, id)
}

func (s *rbsService) UpdateVolume(ctx context.Context, id string, req serverscom.RemoteBlockStorageVolumeUpdateInput) (*serverscom.RemoteBlockStorageVolume, error) {
	return s.client.RemoteBlockStorageVolumes.Update(ctx, id, req)
}

func (s *rbsService) DeleteVolume(ctx context.Context, id string) error {
	return s.client.RemoteBlockStorageVolumes.Delete(ctx, id)
}

func (s *rbsService) GetVolumeCredentials(ctx context.Context, id string) (*serverscom.RemoteBlockStorageVolumeCredentials, error) {
	return s.client.RemoteBlockStorageVolumes.GetCredentials(ctx, id)
}

func (s *rbsService) ResetVolumeCredentials(ctx context.Context, id string) (*serverscom.RemoteBlockStorageVolume, error) {
	return s.client.RemoteBlockStorageVolumes.ResetCredentials(ctx, id)
}

func (s *rbsService) FindLocationByName(ctx context.Context, name string) (*serverscom.Location, error) {
	locations, err := s.client.Locations.Collection().
		SetParam("search_pattern", name).
		Collect(ctx)
	if err != nil {
		return nil, err
	}
	if len(locations) != 0 {
		return &locations[0], nil
	}

	return nil, fmt.Errorf("location with name %s not found", name)
}

func (s *rbsService) FindVolumeByLabel(ctx context.Context, labelKey, labelValue string) (*serverscom.RemoteBlockStorageVolume, error) {
	volumes, err := s.client.RemoteBlockStorageVolumes.Collection().
		SetParam("label_selector", labelKey+"="+labelValue).
		Collect(ctx)
	if err != nil {
		return nil, err
	}
	if len(volumes) == 0 {
		return nil, fmt.Errorf("volume with label %s=%s not found", labelKey, labelValue)
	}

	if len(volumes) > 1 {
		klog.V(1).InfoS("Found volumes with label, returning first one",
			"volumes_count", len(volumes),
			"label_key", labelKey,
			"label_value", labelValue)
	}

	return &volumes[0], nil
}

func (s *rbsService) FindFlavorByName(ctx context.Context, locationID int, name string) (*serverscom.RemoteBlockStorageFlavor, error) {
	flavors, err := s.client.Locations.RemoteBlockStorageFlavors(int64(locationID)).Collect(ctx)
	if err != nil {
		return nil, err
	}

	// use loop because api method doesn't have search_pattern opt
	for _, flavor := range flavors {
		if flavor.Name == name {
			return &flavor, nil
		}
	}

	return nil, fmt.Errorf("flavor with name '%s' not found in location %d", name, locationID)
}
