package detector

import (
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strconv"
	"testing"
	"time"
)

func Test_removeMultipleMasterNodes(t *testing.T) {
	testCases := []struct {
		name          string
		nodes         []corev1.Node
		expectedNodes []corev1.Node
	}{
		{
			name: "test 0 - 1 worker node",
			nodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker1",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleWorker,
						},
					},
				},
			},
			expectedNodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker1",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleWorker,
						},
					},
				},
			},
		},
		{
			name: "test 1 - 1 worker node, 1 master node",
			nodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker1",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleWorker,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "master1",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleMaster,
						},
					},
				},
			},
			expectedNodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker1",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleWorker,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "master1",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleMaster,
						},
					},
				},
			},
		},
		{
			name: "test 2 - 1 worker node, 2 master nodes",
			nodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker1",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleWorker,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "master1",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleMaster,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "master2",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleMaster,
						},
					},
				},
			},
			expectedNodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker1",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleWorker,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "master1",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleMaster,
						},
					},
				},
			},
		},
		{
			name: "test 3 - 1 worker node, 3 master nodes",
			nodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker1",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleWorker,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "master1",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleMaster,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "master2",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleMaster,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "master3",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleMaster,
						},
					},
				},
			},
			expectedNodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker1",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleWorker,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "master1",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleMaster,
						},
					},
				},
			},
		},
		{
			name: "test 4 - 4 worker nodes, 3 master nodes",
			nodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker1",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleWorker,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "master1",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleMaster,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker2",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleWorker,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "master2",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleMaster,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker3",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleWorker,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "master3",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleMaster,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker4",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleWorker,
						},
					},
				},
			},
			expectedNodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker1",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleWorker,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "master1",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleMaster,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker2",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleWorker,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker3",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleWorker,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker4",
						Labels: map[string]string{
							labelNodeRole: labelNodeRoleWorker,
						},
					},
				},
			},
		},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Log(tc.name)

			filteredNodes := removeMultipleMasterNodes(tc.nodes)

			if len(filteredNodes) != len(tc.expectedNodes) {
				t.Fatalf("Expected %d nodes but got %d.\n", len(tc.expectedNodes), len(filteredNodes))
			}

			if !cmp.Equal(filteredNodes, tc.expectedNodes) {
				t.Fatalf("\n\n%s\n", cmp.Diff(tc.expectedNodes, filteredNodes))

			}
		})
	}
}

func Test_maximumNodeTermination(t *testing.T) {
	testCases := []struct {
		name                         string
		nodeCount                    int
		maxNodeTerminationPercentage float64
		expectedNodeCount            int
	}{
		{
			name:                         "test 0 - basic test",
			nodeCount:                    10,
			maxNodeTerminationPercentage: 0.5,
			expectedNodeCount:            5,
		},
		{
			name:                         "test 1 - test minimal limit",
			nodeCount:                    10,
			maxNodeTerminationPercentage: 0.01,
			expectedNodeCount:            1,
		},
		{
			name:                         "test 2 - test minimal limit",
			nodeCount:                    3,
			maxNodeTerminationPercentage: 0.10,
			expectedNodeCount:            1,
		},
		{
			name:                         "test 3 - test rounding",
			nodeCount:                    10,
			maxNodeTerminationPercentage: 0.23,
			expectedNodeCount:            2,
		},
		{
			name:                         "test 4 - lot of nodes",
			nodeCount:                    1000,
			maxNodeTerminationPercentage: 0.25,
			expectedNodeCount:            250,
		},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Log(tc.name)

			maxNodeCount := maximumNodeTermination(tc.nodeCount, tc.maxNodeTerminationPercentage)

			if maxNodeCount != tc.expectedNodeCount {
				t.Fatalf("Expected %d nodes but got %d.\n", tc.expectedNodeCount, maxNodeCount)
			}
		})
	}
}

func Test_nodeNotReady(t *testing.T) {
	testCases := []struct {
		name                 string
		node                 corev1.Node
		expectedNodeNotReady bool
	}{
		{
			name: "test 0 - node ready",
			node: corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{
							Type:              corev1.NodeReady,
							Status:            corev1.ConditionTrue,
							LastHeartbeatTime: metav1.Now(),
						},
					},
				},
			},
			expectedNodeNotReady: false,
		},
		{
			name: "test 1 - node not ready",
			node: corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{
							Type:              corev1.NodeReady,
							Status:            corev1.ConditionFalse,
							LastHeartbeatTime: metav1.Time{Time: time.Now().Add(-time.Minute * 10)},
						},
					},
				},
			},
			expectedNodeNotReady: true,
		},
		{
			name: "test 1 - not ready but only for short time",
			node: corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{
							Type:              corev1.NodeReady,
							Status:            corev1.ConditionFalse,
							LastHeartbeatTime: metav1.Time{Time: time.Now().Add(-time.Second * 10)},
						},
					},
				},
			},
			expectedNodeNotReady: false,
		},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Log(tc.name)

			result := nodeNotReady(tc.node)
			if result != tc.expectedNodeNotReady {
				t.Fatalf("Expected %t but got %t.\n", tc.expectedNodeNotReady, result)
			}
		})
	}
}
