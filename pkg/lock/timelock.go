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

type Config struct {
	Logger    micrologger.Logger
	K8sClient client.Client

	Name string
	TTL  time.Duration
}

type TimeLock struct {
	logger    micrologger.Logger
	k8sClient client.Client

	name string
	ttl  time.Duration
}

// NewTimeLock implements a distributed time lock mechanism mainly used for bad node detection pause period.
// You can inspect the lock annotations on the default namespace in the TC k8s api.
// The lock is unique to each component that request the lock.
//     $ kubectl get ns default --watch | jq '.metadata.annotations'
//     "aws-operator@6.7.0.timelock.giantswarm.io/until": "Mon Jan 2 15:04:05 MST 2006"
//
func New(config Config) (*TimeLock, error) {
	if config.Logger == nil {
		return nil, microerror.Maskf(invalidConfigError, "%T.Logger must not be empty", config)
	}
	if config.K8sClient == nil {
		return nil, microerror.Maskf(invalidConfigError, "%T.K8sClient must not be empty", config)
	}
	if config.Name == "" {
		return nil, microerror.Maskf(invalidConfigError, "%T.Name must not be empty", config)

	}
	if config.TTL == 0 {
		return nil, microerror.Maskf(invalidConfigError, "%T.TTL must not be zero", config)
	}

	d := &TimeLock{
		logger:    config.Logger,
		k8sClient: config.K8sClient,

		name: config.Name,
		ttl:  config.TTL,
	}

	return d, nil
}

func (t *TimeLock) Lock(ctx context.Context) error {
	locked, err := t.isLocked(ctx)
	if err != nil {
		return err
	}

	if locked {
		// fail since lock is already acquired
		return microerror.Maskf(alreadyExistsError, fmt.Sprintf("time lock for the component %s already exists", t.name))
	}

	err = t.createLock(ctx)
	if err != nil {
		return microerror.Mask(err)
	}

	return nil
}

func (t *TimeLock) isLocked(ctx context.Context) (bool, error) {
	var err error
	isLocked := false

	var ns corev1.Namespace

	err = t.k8sClient.Get(ctx, types.NamespacedName{Name: corev1.NamespaceDefault}, &ns)
	if err != nil {
		return false, microerror.Mask(err)
	}

	timeStamp, ok := ns.Annotations[lockName(t.name)]
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

func (t *TimeLock) createLock(ctx context.Context) error {
	var ns corev1.Namespace

	err := t.k8sClient.Get(ctx, types.NamespacedName{Name: corev1.NamespaceDefault}, &ns)
	if err != nil {
		return microerror.Mask(err)
	}
	if ns.Annotations == nil {
		ns.Annotations = map[string]string{}
	}
	// add lock timestamp
	ns.Annotations[lockName(t.name)] = time.Now().Add(t.ttl).Format(time.UnixDate)

	err = t.k8sClient.Update(ctx, &ns)
	if err != nil {
		return microerror.Mask(err)
	}

	return nil
}

func (t *TimeLock) ClearLock(ctx context.Context, component string) error {
	var ns corev1.Namespace

	err := t.k8sClient.Get(ctx, types.NamespacedName{Name: corev1.NamespaceDefault}, &ns)
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
