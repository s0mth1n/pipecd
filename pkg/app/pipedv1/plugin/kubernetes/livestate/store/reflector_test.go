// Copyright 2025 The PipeCD Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	"github.com/pipe-cd/pipecd/pkg/app/pipedv1/plugin/kubernetes/provider"

	"k8s.io/apimachinery/pkg/runtime/schema"

	kubeconfig "github.com/pipe-cd/pipecd/pkg/app/pipedv1/plugin/kubernetes/config"
)

func TestResourceMatcher_matchGVK(t *testing.T) {
	tests := []struct {
		name     string
		config   kubeconfig.KubernetesAppStateInformer
		gvk      schema.GroupVersionKind
		expected bool
	}{
		{
			name: "exclude by APIVersion only",
			config: kubeconfig.KubernetesAppStateInformer{
				ExcludeResources: []kubeconfig.KubernetesResourceMatcher{
					{APIVersion: "apps/v1"},
				},
			},
			gvk:      schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
			expected: false,
		},
		{
			name: "exclude by APIVersion and Kind",
			config: kubeconfig.KubernetesAppStateInformer{
				ExcludeResources: []kubeconfig.KubernetesResourceMatcher{
					{APIVersion: "apps/v1", Kind: "Deployment"},
				},
			},
			gvk:      schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
			expected: false,
		},
		{
			name: "exclude doesn't match different Kind",
			config: kubeconfig.KubernetesAppStateInformer{
				ExcludeResources: []kubeconfig.KubernetesResourceMatcher{
					{APIVersion: "apps/v1", Kind: "Deployment"},
				},
			},
			gvk:      schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "StatefulSet"},
			expected: true, // Should fall back to whitelist check
		},
		{
			name: "include by APIVersion only",
			config: kubeconfig.KubernetesAppStateInformer{
				IncludeResources: []kubeconfig.KubernetesResourceMatcher{
					{APIVersion: "custom.io/v1alpha1"},
				},
			},
			gvk:      schema.GroupVersionKind{Group: "custom.io", Version: "v1alpha1", Kind: "CustomResource"},
			expected: true,
		},
		{
			name: "include by APIVersion and Kind",
			config: kubeconfig.KubernetesAppStateInformer{
				IncludeResources: []kubeconfig.KubernetesResourceMatcher{
					{APIVersion: "custom.io/v1alpha1", Kind: "CustomResource"},
				},
			},
			gvk:      schema.GroupVersionKind{Group: "custom.io", Version: "v1alpha1", Kind: "CustomResource"},
			expected: true,
		},
		{
			name: "include doesn't match different Kind",
			config: kubeconfig.KubernetesAppStateInformer{
				IncludeResources: []kubeconfig.KubernetesResourceMatcher{
					{APIVersion: "custom.io/v1alpha1", Kind: "CustomResource"},
				},
			},
			gvk:      schema.GroupVersionKind{Group: "custom.io", Version: "v1alpha1", Kind: "AnotherResource"},
			expected: false, // Not in whitelist either
		},
		{
			name:     "whitelisted resource - core v1 Pod",
			config:   kubeconfig.KubernetesAppStateInformer{},
			gvk:      schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"},
			expected: true,
		},
		{
			name:     "whitelisted resource - apps/v1 Deployment",
			config:   kubeconfig.KubernetesAppStateInformer{},
			gvk:      schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
			expected: true,
		},
		{
			name:     "non-whitelisted Kind",
			config:   kubeconfig.KubernetesAppStateInformer{},
			gvk:      schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "UnknownKind"},
			expected: false,
		},
		{
			name:     "non-whitelisted Group",
			config:   kubeconfig.KubernetesAppStateInformer{},
			gvk:      schema.GroupVersionKind{Group: "unknown.io", Version: "v1", Kind: "Pod"},
			expected: false,
		},
		{
			name:     "non-whitelisted Version",
			config:   kubeconfig.KubernetesAppStateInformer{},
			gvk:      schema.GroupVersionKind{Group: "apps", Version: "v3", Kind: "Deployment"},
			expected: false,
		},
		{
			name: "exclude takes precedence over include",
			config: kubeconfig.KubernetesAppStateInformer{
				IncludeResources: []kubeconfig.KubernetesResourceMatcher{
					{APIVersion: "apps/v1"},
				},
				ExcludeResources: []kubeconfig.KubernetesResourceMatcher{
					{APIVersion: "apps/v1", Kind: "Deployment"},
				},
			},
			gvk:      schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
			expected: false,
		},
		{
			name: "include different Kind in same APIVersion",
			config: kubeconfig.KubernetesAppStateInformer{
				IncludeResources: []kubeconfig.KubernetesResourceMatcher{
					{APIVersion: "apps/v1"},
				},
				ExcludeResources: []kubeconfig.KubernetesResourceMatcher{
					{APIVersion: "apps/v1", Kind: "Deployment"},
				},
			},
			gvk:      schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "StatefulSet"},
			expected: true, // Included by APIVersion, not excluded by specific Kind
		},
		{
			name:     "v1beta1 version is whitelisted",
			config:   kubeconfig.KubernetesAppStateInformer{},
			gvk:      schema.GroupVersionKind{Group: "extensions", Version: "v1beta1", Kind: "Ingress"},
			expected: true,
		},
		{
			name:     "v1beta2 version is whitelisted",
			config:   kubeconfig.KubernetesAppStateInformer{},
			gvk:      schema.GroupVersionKind{Group: "autoscaling", Version: "v1beta2", Kind: "HorizontalPodAutoscaler"},
			expected: true,
		},
		{
			name:     "v2 version is whitelisted",
			config:   kubeconfig.KubernetesAppStateInformer{},
			gvk:      schema.GroupVersionKind{Group: "autoscaling", Version: "v2", Kind: "HorizontalPodAutoscaler"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher := newResourceMatcher(tt.config)
			result := matcher.matchGVK(tt.gvk)
			if result != tt.expected {
				t.Errorf("matchGVK() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestNewResourceMatcher(t *testing.T) {
	config := kubeconfig.KubernetesAppStateInformer{
		IncludeResources: []kubeconfig.KubernetesResourceMatcher{
			{APIVersion: "apps/v1"},
			{APIVersion: "custom.io/v1alpha1", Kind: "CustomResource"},
		},
		ExcludeResources: []kubeconfig.KubernetesResourceMatcher{
			{APIVersion: "batch/v1"},
			{APIVersion: "apps/v1", Kind: "Deployment"},
		},
	}

	matcher := newResourceMatcher(config)

	// Test includes
	if _, ok := matcher.includes["apps/v1"]; !ok {
		t.Error("Expected 'apps/v1' to be in includes")
	}
	if _, ok := matcher.includes["custom.io/v1alpha1:CustomResource"]; !ok {
		t.Error("Expected 'custom.io/v1alpha1:CustomResource' to be in includes")
	}

	// Test excludes
	if _, ok := matcher.excludes["batch/v1"]; !ok {
		t.Error("Expected 'batch/v1' to be in excludes")
	}
	if _, ok := matcher.excludes["apps/v1:Deployment"]; !ok {
		t.Error("Expected 'apps/v1:Deployment' to be in excludes")
	}

	// Test counts
	if len(matcher.includes) != 2 {
		t.Errorf("Expected 2 includes, got %d", len(matcher.includes))
	}
	if len(matcher.excludes) != 2 {
		t.Errorf("Expected 2 excludes, got %d", len(matcher.excludes))
	}
}

func TestReflector_OnDelete(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		deleted     any
		want        []types.UID
		wantManaged []types.UID
		noFill      bool
	}{
		{name: "managed resource", deleted: "managed", want: []types.UID{"child"}},
		{name: "managed resource tombstone", deleted: cache.DeletedFinalStateUnknown{Key: "managed"}, want: []types.UID{"child"}},
		{name: "child resource", deleted: "child", want: []types.UID{"managed"}, wantManaged: []types.UID{"managed"}},
		{name: "child resource tombstone", deleted: cache.DeletedFinalStateUnknown{Key: "child"}, want: []types.UID{"managed"}, wantManaged: []types.UID{"managed"}},
		{name: "missing tombstone object", deleted: cache.DeletedFinalStateUnknown{Key: "missing"}, noFill: true, want: []types.UID{"managed", "child"}, wantManaged: []types.UID{"managed"}},
		{name: "invalid object", deleted: 1, want: []types.UID{"managed", "child"}, wantManaged: []types.UID{"managed"}},
		{name: "invalid tombstone", deleted: cache.DeletedFinalStateUnknown{Key: "managed", Obj: 1}, want: []types.UID{"managed", "child"}, wantManaged: []types.UID{"managed"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			managed := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment"}}
			managed.SetName("managed")
			managed.SetUID("managed")
			managed.SetAnnotations(map[string]string{provider.LabelApplication: "app", provider.LabelManagedBy: provider.ManagedByPiped})
			child := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod"}}
			child.SetName("child")
			child.SetUID("child")
			child.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "managed", UID: "managed"}})
			resources := newDeployTargetResources("target")
			r := &reflector{resourceEventHandler: resources, logger: zap.NewNop()}
			r.OnAdd(managed, false)
			r.OnAdd(child, false)
			before, err := resources.Livestate(context.Background(), "app")
			require.NoError(t, err)
			require.Len(t, before, 2)
			deleted := tt.deleted
			objects := map[string]*unstructured.Unstructured{"managed": managed, "child": child}
			switch v := deleted.(type) {
			case string:
				deleted = objects[v]
			case cache.DeletedFinalStateUnknown:
				if v.Obj == nil && !tt.noFill {
					v.Obj = objects[v.Key]
				}
				deleted = v
			}
			r.OnDelete(deleted)
			after, err := resources.Livestate(context.Background(), "app")
			require.NoError(t, err)
			ids := make([]types.UID, 0, len(after))
			managedIDs := make([]types.UID, 0, len(after))
			for _, m := range after {
				ids = append(ids, m.UID())
				if m.IsManagedByPiped() {
					managedIDs = append(managedIDs, m.UID())
				}
			}
			assert.ElementsMatch(t, tt.want, ids)
			assert.ElementsMatch(t, tt.wantManaged, managedIDs)
			resources.mu.RLock()
			_, exists := resources.resources["managed"]
			_, childExists := resources.resources["child"]
			resources.mu.RUnlock()
			assert.Equal(t, len(tt.wantManaged) > 0, exists)
			assert.Equal(t, slices.Contains(tt.want, types.UID("child")), childExists)
		})
	}
}
