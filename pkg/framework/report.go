/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package framework

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
)

const (
	ResourceNvidiaGPU v1.ResourceName = "nvdia.com/gpu"
)

type ClusterCapacityReview struct {
	metav1.TypeMeta
	Spec   ClusterCapacityReviewSpec   `json:"spec"`
	Status ClusterCapacityReviewStatus `json:"status"`
}

type ClusterCapacityReviewSpec struct {
	// the pod desired for scheduling
	Templates []v1.Pod `json:"templates"`

	// desired number of replicas that should be scheduled
	// +optional
	Replicas int32 `json:"replicas"`

	PodRequirements []*Requirements `json:"podRequirements"`
}

type ClusterCapacityReviewStatus struct {
	CreationTimestamp time.Time `json:"creationTimestamp"`
	// actual number of replicas that could schedule
	Replicas int32 `json:"replicas"`

	StopReason *ClusterCapacityReviewScheduleFailReason `json:"stopReason"`

	// per node information about the scheduling simulation
	Pods []*ClusterCapacityReviewResult `json:"pods"`
}

type ClusterCapacityReviewResult struct {
	PodName string `json:"podName"`
	// numbers of replicas on nodes
	ReplicasOnNodes []*ReplicasOnNode `json:"replicasOnNodes"`
	// reason why no more pods could schedule (if any on this node)
	FailSummary []FailReasonSummary `json:"failSummary"`
}

type ReplicasOnNode struct {
	NodeName string `json:"nodeName"`
	Replicas int    `json:"replicas"`
}

type FailReasonSummary struct {
	Reason string `json:"reason"`
	Count  int    `json:"count"`
}

type Resources struct {
	PrimaryResources v1.ResourceList           `json:"primaryResources"`
	ScalarResources  map[v1.ResourceName]int64 `json:"scalarResources"`
}

type Requirements struct {
	PodName       string            `json:"podName"`
	Resources     *Resources        `json:"resources"`
	NodeSelectors map[string]string `json:"nodeSelectors"`
}

type ClusterCapacityReviewScheduleFailReason struct {
	FailType    string `json:"failType"`
	FailMessage string `json:"failMessage"`
}

func getMainStopReason(message string) *ClusterCapacityReviewScheduleFailReason {
	slicedMessage := strings.Split(message, "\n")
	colon := strings.Index(slicedMessage[0], ":")

	fail := &ClusterCapacityReviewScheduleFailReason{
		FailType:    slicedMessage[0][:colon],
		FailMessage: strings.Trim(slicedMessage[0][colon+1:], " "),
	}
	return fail
}

func getResourceRequest(pod *v1.Pod) *Resources {
	result := Resources{
		PrimaryResources: v1.ResourceList{
			v1.ResourceName(v1.ResourceCPU):    *resource.NewMilliQuantity(0, resource.DecimalSI),
			v1.ResourceName(v1.ResourceMemory): *resource.NewQuantity(0, resource.BinarySI),
			v1.ResourceName(ResourceNvidiaGPU): *resource.NewMilliQuantity(0, resource.DecimalSI),
		},
	}

	for _, container := range pod.Spec.Containers {
		for rName, rQuantity := range container.Resources.Requests {
			switch rName {
			case v1.ResourceMemory:
				rQuantity.Add(*(result.PrimaryResources.Memory()))
				result.PrimaryResources[v1.ResourceMemory] = rQuantity
			case v1.ResourceCPU:
				rQuantity.Add(*(result.PrimaryResources.Cpu()))
				result.PrimaryResources[v1.ResourceCPU] = rQuantity
				//case v1.ResourceNvidiaGPU:
				//	rQuantity.Add(*(result.PrimaryResources.NvidiaGPU()))
				//	result.PrimaryResources[v1.ResourceNvidiaGPU] = rQuantity
			default:
				if schedutil.IsScalarResourceName(rName) {
					// Lazily allocate this map only if required.
					if result.ScalarResources == nil {
						result.ScalarResources = map[v1.ResourceName]int64{}
					}
					result.ScalarResources[rName] += rQuantity.Value()
				}
			}
		}
	}
	return &result
}

func parsePodsReview(templatePods []*v1.Pod, status Status) []*ClusterCapacityReviewResult {
	result := make([]*ClusterCapacityReviewResult, len(status.Pods))
	for i, pod := range status.Pods {
		result[i] = &ClusterCapacityReviewResult{
			PodName: pod.Namespace + "/" + pod.Name,
		}
		if pod.Spec.NodeName != "" {
			result[i].ReplicasOnNodes = []*ReplicasOnNode{
				{
					NodeName: pod.Spec.NodeName,
					Replicas: 1,
				},
			}
		}

		for _, podCondition := range pod.Status.Conditions {
			if podCondition.Type == v1.PodScheduled && podCondition.Status == v1.ConditionFalse && podCondition.Reason == "Unschedulable" {
				result[i].FailSummary = []FailReasonSummary{
					{
						Reason: podCondition.Message,
					},
				}
			}
		}
	}
	return result
}

func getPodsRequirements(pods []*v1.Pod) []*Requirements {
	result := make([]*Requirements, 0)
	for _, pod := range pods {
		podRequirements := &Requirements{
			PodName:       pod.Name,
			Resources:     getResourceRequest(pod),
			NodeSelectors: pod.Spec.NodeSelector,
		}
		result = append(result, podRequirements)
	}
	return result
}

func deepCopyPods(in []*v1.Pod, out []v1.Pod) {
	for i, pod := range in {
		out[i] = *pod.DeepCopy()
	}
}

func getReviewSpec(podTemplates []*v1.Pod) ClusterCapacityReviewSpec {

	podCopies := make([]v1.Pod, len(podTemplates))
	deepCopyPods(podTemplates, podCopies)
	return ClusterCapacityReviewSpec{
		Templates:       podCopies,
		PodRequirements: getPodsRequirements(podTemplates),
	}
}

func getReviewStatus(pods []*v1.Pod, status Status) ClusterCapacityReviewStatus {
	return ClusterCapacityReviewStatus{
		CreationTimestamp: time.Now(),
		Replicas:          int32(len(status.Pods)),
		// StopReason:        getMainStopReason(status.StopReason),
		Pods: parsePodsReview(pods, status),
	}
}

func GetReport(pods []*v1.Pod, status Status) *ClusterCapacityReview {
	return &ClusterCapacityReview{
		Spec:   getReviewSpec(pods),
		Status: getReviewStatus(pods, status),
	}
}

func instancesSum(replicasOnNodes []*ReplicasOnNode) int {
	result := 0
	for _, v := range replicasOnNodes {
		result += v.Replicas
	}
	return result
}

func clusterCapacityReviewPrettyPrint(r *ClusterCapacityReview, verbose bool) {
	// if verbose {
	// 	for _, req := range r.Spec.PodRequirements {
	// 		fmt.Printf("%v pod requirements:\n", req.PodName)
	// 		fmt.Printf("\t- CPU: %v\n", req.Resources.PrimaryResources.Cpu().String())
	// 		fmt.Printf("\t- Memory: %v\n", req.Resources.PrimaryResources.Memory().String())
	// 		//if !req.Resources.PrimaryResources.NvidiaGPU().IsZero() {
	// 		//	fmt.Printf("\t- NvidiaGPU: %v\n", req.Resources.PrimaryResources.NvidiaGPU().String())
	// 		//}
	// 		if req.Resources.ScalarResources != nil {
	// 			fmt.Printf("\t- ScalarResources: %v\n", req.Resources.ScalarResources)
	// 		}

	// 		if req.NodeSelectors != nil {
	// 			fmt.Printf("\t- NodeSelector: %v\n", labels.SelectorFromSet(labels.Set(req.NodeSelectors)).String())
	// 		}
	// 		fmt.Printf("\n")
	// 	}
	// }

	// if verbose {
	// 	fmt.Printf("\nStop reason: %v: %v\n", r.Status.StopReason.FailType, r.Status.StopReason.FailMessage)
	// }

	failDetailReport := strings.Builder{}
	succeedDeetailReport := strings.Builder{}
	shortReport := strings.Builder{}
	failedCount, succeededCount := 0, 0
	if verbose && r.Status.Replicas > 0 {
		for _, pod := range r.Status.Pods {
			if len(pod.FailSummary) > 0 {
				failDetailReport.WriteString(fmt.Sprintf("%v\n", pod.PodName))
				for _, fs := range pod.FailSummary {
					failDetailReport.WriteString(fmt.Sprintf("\t- %v\n", fs.Reason))
				}
				shortReport.WriteString(fmt.Sprintf("failed    to schedule pod %v.\n", pod.PodName))
				failedCount++
			}
			if len(pod.ReplicasOnNodes) > 0 {
				succeedDeetailReport.WriteString(fmt.Sprintf("%v\n", pod.PodName))
				for _, ron := range pod.ReplicasOnNodes {
					succeedDeetailReport.WriteString(fmt.Sprintf("\t- %v\n", ron.NodeName))
				}
				shortReport.WriteString(fmt.Sprintf("succeeded to schedule pod %v.\n", pod.PodName))
				succeededCount++
			}
		}
	}

	fmt.Printf("\nPod scheduled fail:\n")
	fmt.Printf("%v", failDetailReport.String())

	fmt.Printf("\nPod distribution among nodes:\n")
	fmt.Printf("%v", succeedDeetailReport.String())

	fmt.Printf("\n%v", shortReport.String())
	fmt.Printf("\n%v pods should be scheduled, %v failed, %v succeeded.\n", len(r.Status.Pods), failedCount, succeededCount)
}

func clusterCapacityReviewPrintJson(r *ClusterCapacityReview) error {
	jsoned, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("Failed to create json: %v", err)
	}
	fmt.Println(string(jsoned))
	return nil
}

func clusterCapacityReviewPrintYaml(r *ClusterCapacityReview) error {
	yamled, err := yaml.Marshal(r)
	if err != nil {
		return fmt.Errorf("Failed to create yaml: %v", err)
	}
	fmt.Print(string(yamled))
	return nil
}

func ClusterCapacityReviewPrint(r *ClusterCapacityReview, verbose bool, format string) error {
	switch format {
	case "json":
		return clusterCapacityReviewPrintJson(r)
	case "yaml":
		return clusterCapacityReviewPrintYaml(r)
	case "":
		clusterCapacityReviewPrettyPrint(r, verbose)
		return nil
	default:
		return fmt.Errorf("output format %q not recognized", format)
	}
}
