package detector

import (
	"context"
	"fmt"
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

	AnnotationNodeNotReadyTick = "giantswarm.io/node-not-ready-tick"
	LabelNodeRole              = "role"
	LabelNodeRoleMaster        = "master"
)

type Config struct {
	Logger    micrologger.Logger
	K8sClient client.Client

	MaxNodeTerminationPercentage float64
	NotReadyTickThreshold        int
	PauseBetweenTermination      time.Duration
}

type Detector struct {
	logger    micrologger.Logger
	k8sClient client.Client

	maxNodeTerminationPercentage float64
	notReadyTickThreshold        int
	pauseBetweenTermination      time.Duration
}

// NewTimeLock implements a distributed time lock mechanism mainly used for bad node detection pause period.
// You can inspect the lock annotations on the default namespace in the TC k8s api.
// The lock is unique to each component that request the lock.
//     $ kubectl get ns default --watch | jq '.metadata.annotations'
//     "aws-operator@6.7.0.timelock.giantswarm.io/until": "Mon Jan 2 15:04:05 MST 2006"
//
func NewDetector(config Config) (*Detector, error) {
	if config.Logger == nil {
		return nil, microerror.Maskf(invalidConfigError, "%T.Logger must not be empty", config)
	}
	if config.K8sClient == nil {
		return nil, microerror.Maskf(invalidConfigError, "%T.K8sClient must not be empty", config)
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

		notReadyTickThreshold:   config.NotReadyTickThreshold,
		pauseBetweenTermination: config.PauseBetweenTermination,
	}

	return d, nil
}

func (d *Detector) DetectBadNodes(ctx context.Context, obj interface{}) ([]corev1.Node, error) {
	var nodeList corev1.NodeList
	{
		err := d.k8sClient.List(ctx, &nodeList)
		if err != nil {
			return nil, microerror.Mask(err)
		}
	}

	nodesToTerminate, err := d.detectBadNodes(ctx, nodeList.Items)
	if err != nil {
		return nil, microerror.Mask(err)
	}

	// remove additional master nodes to avoid multiple master node termination at the same time
	nodesToTerminate = d.removeMultipleMasterNodes(ctx, nodesToTerminate)
	d.logger.LogCtx(ctx, "level", "debug", "message", fmt.Sprintf("found %d nodes marked for termination", len(nodesToTerminate)))

	// check for node termination limit, to prevent termination of all nodes at once
	maxNodeTermination := d.maximumNodeTermination(len(nodeList.Items))
	if len(nodesToTerminate) > maxNodeTermination {
		nodesToTerminate = nodesToTerminate[:maxNodeTermination]
		d.logger.LogCtx(ctx, "level", "debug", "message", fmt.Sprintf("limited node termination to %d nodes", maxNodeTermination))
	}

	return nodesToTerminate, nil
}

// detectBadNodes looks at all nodes and adds a tick counter on each node if it sees it as NotReady
// It will return all nodes which reached threshold for the tick counter.
func (d *Detector) detectBadNodes(ctx context.Context, nodes []corev1.Node) ([]corev1.Node, error) {
	var badNodes []corev1.Node
	for _, n := range nodes {
		//
		notReadyTickCount, err := d.updateNodeNotReadyTickAnnotations(ctx, n)
		if err != nil {
			return nil, microerror.Mask(err)
		}

		if notReadyTickCount >= d.notReadyTickThreshold {
			badNodes = append(badNodes, n)
		}
	}
	return badNodes, nil
}

// nodeNotReady returns true of the node is not ready for certain period of time
// this is used to detect bad nodes
func nodeNotReady(n corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		// find kubelet "ready" condition
		if c.Type == "Ready" && c.Status != "True" {
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
// Each reconcilation loop can increase or decrease the tick count by 1.
func (d *Detector) updateNodeNotReadyTickAnnotations(ctx context.Context, n corev1.Node) (int, error) {
	var err error

	// fetch current notReady tick count from node
	// if there is no annotation yet, the value will be 0
	notReadyTickCount := 0
	{
		tick, ok := n.Annotations[AnnotationNodeNotReadyTick]
		if ok {
			notReadyTickCount, err = strconv.Atoi(tick)
			if err != nil {
				return -1, microerror.Mask(err)
			}
		}
	}

	updated := false
	// increase or decrease the tick count depending on the node status
	if nodeNotReady(n) {
		notReadyTickCount++
		updated = true
	} else if notReadyTickCount > 0 {
		notReadyTickCount--
		updated = true
	}

	if updated {
		// update the tick count on the node
		n.Annotations[AnnotationNodeNotReadyTick] = fmt.Sprintf("%d", notReadyTickCount)
		err = d.k8sClient.Update(ctx, &n)
		if err != nil {
			return -1, microerror.Mask(err)
		}
		d.logger.LogCtx(ctx, "level", "debug", "message", fmt.Sprintf("updated not ready tick count to %d/%d for node %s", notReadyTickCount, d.notReadyTickThreshold, n.Name))
	}
	return notReadyTickCount, nil
}

// maximumNodeTermination calculates the maximum number of nodes that can be terminated in single loop
// the number is calculated with help of key.NodeAutoRepairTerminationPercentage
// which determines how much percentage of nodes can be terminated
// the minimum is 1 node termination per reconciliation loop
func (d *Detector) maximumNodeTermination(nodeCount int) int {
	limit := math.Round(float64(nodeCount) * d.maxNodeTerminationPercentage)

	if limit < 1 {
		limit = 1
	}
	return int(limit)
}

// removeMultipleMasterNodes removes multiple master nodes from the list to avoid more than 1 master node termination at same time
func (d *Detector) removeMultipleMasterNodes(ctx context.Context, nodeList []corev1.Node) []corev1.Node {
	foundMasterNode := false
	var filteredNodes []corev1.Node

	for _, n := range nodeList {
		if n.Labels[LabelNodeRole] == LabelNodeRoleMaster {
			if !foundMasterNode {
				filteredNodes = append(filteredNodes, n)
				foundMasterNode = true
			} else {
				// removing additional master nodes from the list
				d.logger.LogCtx(ctx, "level", "debug", "message", fmt.Sprintf("removed master node %s from node termination list to avoid multiple master nodes termination at once", n.Name))
				continue
			}
		} else {
			filteredNodes = append(filteredNodes, n)
		}
	}
	return filteredNodes
}
