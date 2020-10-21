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

	annotationNodeNotReadyTick = "giantswarm.io/node-not-ready-tick"
	labelNodeRole              = "role"
	labelNodeRoleMaster        = "master"
)

type Config struct {
	Logger    micrologger.Logger
	K8sClient client.Client

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

		maxNodeTerminationPercentage: config.MaxNodeTerminationPercentage,
		notReadyTickThreshold:        config.NotReadyTickThreshold,
		pauseBetweenTermination:      config.PauseBetweenTermination,
	}

	return d, nil
}

// DetectBadNodes will return list of nodes that should be terminated which in documentation terminology is used as 'marked for termination'.
func (d *Detector) DetectBadNodes(ctx context.Context) ([]corev1.Node, error) {
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
func (d *Detector) updateNodeNotReadyTickAnnotations(ctx context.Context, n corev1.Node) (int, error) {
	var err error
	// fetch current notReady tick count from node
	// if there is no annotation yet, the value will be 0
	notReadyTickCount := 0
	{
		tick, ok := n.Annotations[annotationNodeNotReadyTick]
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
		n.Annotations[annotationNodeNotReadyTick] = fmt.Sprintf("%d", notReadyTickCount)
		err = d.k8sClient.Update(ctx, &n)
		if err != nil {
			return -1, microerror.Mask(err)
		}
		d.logger.LogCtx(ctx, "level", "debug", "message", fmt.Sprintf("updated not ready tick count to %d/%d for node %s", notReadyTickCount, d.notReadyTickThreshold, n.Name))
	}
	return notReadyTickCount, nil
}

// maximumNodeTermination calculates the maximum number of nodes that can be terminated on single run
// the number is calculated with help of d.maxNodeTerminationPercentage
// which determines how much percentage of nodes can be terminated
// the minimum is 1 node termination per run
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
	// filteredNodes list will contain maximum 1 master node at the end of the function
	var filteredNodes []corev1.Node

	for _, n := range nodeList {
		if n.Labels[labelNodeRole] == labelNodeRoleMaster {
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
