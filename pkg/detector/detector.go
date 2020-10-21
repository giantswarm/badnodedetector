package detector

import (
	"context"
	"fmt"
	"github.com/giantswarm/badnodedetector/pkg/lock"
	"math"
	"strconv"
	"time"

	"github.com/giantswarm/microerror"
	"github.com/giantswarm/micrologger"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	defaultMaxNodeTerminationPercentage = 0.10
	defaultNotReadyTickThreshold        = 6
	defaultPauseBetweenTermination      = time.Minute * 10

	nodeNotReadyDuration = time.Second * 30

	annotationNodeNotReadyTick = "giantswarm.io/node-not-ready-tick"
	labelNodeRole              = "role"
	labelNodeRoleMaster        = "master"
	labelNodeRoleWorker        = "worker"
)

type Config struct {
	Logger    micrologger.Logger
	K8sClient client.Client

	LockName string
	// MaxNodeTerminationPercentage defines a maximum percentage of nodes that will be returned as 'marked for termination'
	// ie: if the value is 0.5 and cluster have 10 nodes, than `DetectBadNodes`can only return maximum of 5 nodes
	// marked for termination at single run
	MaxNodeTerminationPercentage float64
	// NotReadyTickThreshold defines a how many times the node must bee seen as NotReady in order to return it as 'marked for termination'
	NotReadyTickThreshold int
	// PauseBetweenTermination defines a pause between 2 intervals where node termination can occur.
	// This is a safeguard to prevent nodes being terminated over and over or to not terminate too much at once.
	// ie: if the value is 5m it means once it returned nodes for termination it wont return another nodes for another 5 min.
	PauseBetweenTermination time.Duration
}

type Detector struct {
	logger    micrologger.Logger
	k8sClient client.Client

	lockName                     string
	maxNodeTerminationPercentage float64
	notReadyTickThreshold        int
	pauseBetweenTermination      time.Duration
}

func NewDetector(config Config) (*Detector, error) {
	if config.Logger == nil {
		return nil, microerror.Maskf(invalidConfigError, "%T.Logger must not be empty", config)
	}
	if config.K8sClient == nil {
		return nil, microerror.Maskf(invalidConfigError, "%T.K8sClient must not be empty", config)
	}
	if config.LockName == "" {
		return nil, microerror.Maskf(invalidConfigError, "%T.LockName must not be empty", config)
	}

	if config.MaxNodeTerminationPercentage == 0 {
		config.MaxNodeTerminationPercentage = defaultMaxNodeTerminationPercentage
	}
	if config.NotReadyTickThreshold == 0 {
		config.NotReadyTickThreshold = defaultNotReadyTickThreshold
	}
	if config.PauseBetweenTermination == 0 {
		config.PauseBetweenTermination = defaultPauseBetweenTermination
	}

	d := &Detector{
		logger:    config.Logger,
		k8sClient: config.K8sClient,

		lockName:                     config.LockName,
		maxNodeTerminationPercentage: config.MaxNodeTerminationPercentage,
		notReadyTickThreshold:        config.NotReadyTickThreshold,
		pauseBetweenTermination:      config.PauseBetweenTermination,
	}

	return d, nil
}

// DetectBadNodes will return list of nodes that should be terminated which in documentation terminology is used as 'marked for termination'.
func (d *Detector) DetectBadNodes(ctx context.Context) ([]corev1.Node, error) {
	var err error
	var timeLock *lock.TimeLock
	{
		config := lock.TimeLockConfig{
			Logger:    d.logger,
			K8sClient: d.k8sClient,

			Name: d.lockName,
			TTL:  d.pauseBetweenTermination,
		}

		timeLock, err = lock.NewTimeLock(config)
		if err != nil {
			return nil, microerror.Mask(err)
		}
	}

	var nodeList corev1.NodeList
	{
		err := d.k8sClient.List(ctx, &nodeList)
		if err != nil {
			return nil, microerror.Mask(err)
		}
	}

	// badNodes list will contain all nodes that reached tick threshold and are 'marked for termination'
	var badNodes []corev1.Node
	for _, n := range nodeList.Items {
		notReadyTickCount, updated := nodeNotReadyTickCount(n)

		if notReadyTickCount >= d.notReadyTickThreshold {
			badNodes = append(badNodes, n)
		}

		// if the tick counter changed, we need to update the value in the k8s api
		if updated {
			// update the tick count on the node
			n.Annotations[annotationNodeNotReadyTick] = fmt.Sprintf("%d", notReadyTickCount)
			err := d.k8sClient.Update(ctx, &n)
			if err != nil {
				return nil, microerror.Mask(err)
			}
			d.logger.LogCtx(ctx, "level", "debug", "message", fmt.Sprintf("updated not ready tick count to %d/%d for node %s", notReadyTickCount, d.notReadyTickThreshold, n.Name))
		}
	}

	// remove additional master nodes to avoid multiple master node termination at the same time
	badNodes = removeMultipleMasterNodes(badNodes)
	d.logger.LogCtx(ctx, "level", "debug", "message", fmt.Sprintf("found %d nodes marked for termination", len(badNodes)))

	// check for node termination limit, to prevent termination of all nodes at once
	maxNodeTermination := maximumNodeTermination(len(nodeList.Items), d.maxNodeTerminationPercentage)
	if len(badNodes) > maxNodeTermination {
		badNodes = badNodes[:maxNodeTermination]
		d.logger.LogCtx(ctx, "level", "debug", "message", fmt.Sprintf("limited node termination to %d nodes", maxNodeTermination))
	}

	err = timeLock.Lock(ctx)
	if lock.IsAlreadyExists(err) {
		d.logger.LogCtx(ctx, "level", "debug", "message", "skipping node termination due to pause period between another termination")

	} else if err != nil {
		return nil, microerror.Mask(err)
	}

	return badNodes, nil
}

// nodeNotReady returns true of the node is not ready for certain period of time
// this is used to detect bad nodes
func nodeNotReady(n corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		// find kubelet "ready" condition
		if c.Type == corev1.NodeReady && c.Status != corev1.ConditionTrue {
			// kubelet must be in NotReady at least for some time to avoid quick flaps
			if time.Since(c.LastHeartbeatTime.Time) >= nodeNotReadyDuration {
				return true
			}
		}
	}
	return false
}

// updateNodeNotReadyTickAnnotations will update annotations on the node
// depending if the node is Ready or not
// the annotation is used to track how many times node was seen as not ready
// and in case it will reach a threshold, the node will be marked for termination.
// Each run of this function can increase or decrease the tick count by 1.
// function return a tick counter (int) and a bool indicating if the value changed
func nodeNotReadyTickCount(n corev1.Node) (int, bool) {
	var err error
	updated := false

	// fetch current notReady tick count from node
	// if there is no annotation yet, the value will be 0
	notReadyTickCount := 0
	{
		tick, ok := n.Annotations[annotationNodeNotReadyTick]
		if ok {
			notReadyTickCount, err = strconv.Atoi(tick)
			// in case the annotation is a garbage lets reset to 0 and update it
			if err != nil {
				notReadyTickCount = 0
				updated = true
			}
		}
	}

	// increase or decrease the tick count depending on the node status
	if nodeNotReady(n) {
		notReadyTickCount++
		updated = true
	} else if notReadyTickCount > 0 {
		notReadyTickCount--
		updated = true
	}

	return notReadyTickCount, updated
}

// maximumNodeTermination calculates the maximum number of nodes that can be terminated on single run
// the number is calculated with help of maxNodeTerminationPercentage
// which determines how much percentage of nodes can be terminated
// the minimum is 1 node termination per run
func maximumNodeTermination(nodeCount int, maxNodeTerminationPercentage float64) int {
	limit := math.Round(float64(nodeCount) * maxNodeTerminationPercentage)

	if limit < 1 {
		limit = 1
	}
	return int(limit)
}

// removeMultipleMasterNodes removes multiple master nodes from the list to avoid more than 1 master node termination at same time
// worker nodes in the list are unaffected
func removeMultipleMasterNodes(nodeList []corev1.Node) []corev1.Node {
	foundMasterNode := false
	// filteredNodes list will contain maximum 1 master node and unlimited number of worker nodes at the end of the function
	var filteredNodes []corev1.Node

	for _, n := range nodeList {
		if n.Labels[labelNodeRole] == labelNodeRoleMaster {
			// append only the first master that is found in the list
			// any following master is not appended to the final list
			if !foundMasterNode {
				filteredNodes = append(filteredNodes, n)
				foundMasterNode = true
			} else {
				// removing additional master nodes from the list
				continue
			}
		} else {
			// append all non-master nodes
			filteredNodes = append(filteredNodes, n)
		}
	}
	return filteredNodes
}
