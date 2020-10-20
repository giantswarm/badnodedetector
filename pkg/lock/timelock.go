package lock

import (
	"context"
	"fmt"
	"time"

	"github.com/giantswarm/microerror"
	"github.com/giantswarm/micrologger"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	timeLockName = "timelock.giantswarm.io/until"
)

type TimeLockConfig struct {
	Logger    micrologger.Logger
	K8sClient client.Client

	ClusterID          string
	ClusterCRNamespace string
	TTL                time.Duration
}

type TimeLock struct {
	logger    micrologger.Logger
	k8sClient client.Client

	clusterID          string
	clusterCRNamespace string
	ttl                time.Duration
}

// NewTimeLock implements a distributed time lock mechanism mainly used for bad node detection pause period.
// It expect that the namespace with the cluster id already exists.
// You can inspect the lock annotations on the k8s object.
// The lock is unique to each cluster ID.
//     $ kubectl get ns CLUSTER_ID --watch | jq '.metadata.annotations'
//     "aws-operator@6.7.0.timelock.giantswarm.io/until": "Mon Jan 2 15:04:05 MST 2006"
//
func NewTimeLock(config TimeLockConfig) (*TimeLock, error) {
	if config.Logger == nil {
		return nil, microerror.Maskf(invalidConfigError, "%T.Logger must not be empty", config)
	}
	if config.K8sClient == nil {
		return nil, microerror.Maskf(invalidConfigError, "%T.K8sClient must not be empty", config)
	}
	if config.ClusterID == "" {
		return nil, microerror.Maskf(invalidConfigError, "%T.ClusterID must not be empty", config)
	}
	if config.TTL == 0 {
		return nil, microerror.Maskf(invalidConfigError, "%T.TTL must not be zero", config)
	}

	d := &TimeLock{
		logger:    config.Logger,
		k8sClient: config.K8sClient,

		clusterID:          config.ClusterID,
		clusterCRNamespace: config.ClusterCRNamespace,
		ttl:                config.TTL,
	}

	return d, nil
}

func (t *TimeLock) Lock(ctx context.Context, component string) error {
	locked, err := t.isLocked(ctx, component)
	if err != nil {
		return err
	}

	if locked {
		// fail since lock is already acquired
		return microerror.Maskf(alreadyExistsError, fmt.Sprintf("time lock for cluster %s already exists", t.clusterID))
	}

	err = t.createLock(ctx, component)
	if err != nil {
		return microerror.Mask(err)
	}

	return nil
}

func (t *TimeLock) isLocked(ctx context.Context, component string) (bool, error) {
	var err error
	isLocked := false

	var ns corev1.Namespace

	err = t.k8sClient.Get(ctx, types.NamespacedName{Namespace: t.clusterCRNamespace, Name: t.clusterID}, &ns)
	if err != nil {
		return false, microerror.Mask(err)
	}

	timeStamp, ok := ns.Annotations[lockName(component)]
	if ok {
		ts, err := time.Parse(time.UnixDate, timeStamp)
		if err != nil {
			return false, microerror.Mask(err)
		}
		// check if the lock is expired
		if time.Now().Before(ts) {
			isLocked = true
		}
	}
	return isLocked, nil
}

func (t *TimeLock) createLock(ctx context.Context, component string) error {
	var ns corev1.Namespace

	err := t.k8sClient.Get(ctx, types.NamespacedName{Namespace: t.clusterCRNamespace, Name: t.clusterID}, &ns)
	if err != nil {
		return microerror.Mask(err)
	}
	// add lock timestamp
	ns.Annotations[lockName(component)] = time.Now().Add(t.ttl).Format(time.UnixDate)

	err = t.k8sClient.Update(ctx, &ns)
	if err != nil {
		return microerror.Mask(err)
	}

	return nil
}

func (t *TimeLock) ClearLock(ctx context.Context, component string) error {
	var ns corev1.Namespace

	err := t.k8sClient.Get(ctx, types.NamespacedName{Namespace: t.clusterCRNamespace, Name: t.clusterID}, &ns)
	if err != nil {
		return microerror.Mask(err)
	}

	updated := false

	if _, ok := ns.Annotations[lockName(component)]; ok {
		// delete lock from annotations
		delete(ns.Annotations, component)
		updated = true
	}

	if updated {
		err = t.k8sClient.Update(ctx, &ns)
		if err != nil {
			return microerror.Mask(err)
		}
	}
	return nil
}

func lockName(component string) string {
	return fmt.Sprintf("%s.%s", component, timeLockName)
}
