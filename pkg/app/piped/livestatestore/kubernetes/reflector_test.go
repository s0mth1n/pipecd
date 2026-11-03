// Copyright 2024 The PipeCD Authors.
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

package kubernetes

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"

	provider "github.com/pipe-cd/pipecd/pkg/app/piped/platformprovider/kubernetes"

	"github.com/pipe-cd/pipecd/pkg/config"
)

func TestResourceMatcher(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name string
		cfg  config.KubernetesAppStateInformer
		gvks map[schema.GroupVersionKind]bool
	}{
		{
			name: "empty config",
			cfg:  config.KubernetesAppStateInformer{},
			gvks: map[schema.GroupVersionKind]bool{
				{"pipecd.dev", "v1beta1", "Foo"}:       false,
				{"", "v1", "Foo"}:                      false,
				{"", "v1", "Service"}:                  true,
				{"networking.k8s.io", "v1", "Ingress"}: true,
			},
		},
		{
			name: "include config",
			cfg: config.KubernetesAppStateInformer{
				IncludeResources: []config.KubernetesResourceMatcher{
					{APIVersion: "pipecd.dev/v1beta1"},
					{APIVersion: "pipecd.dev/v1alpha1", Kind: "Foo"},
				},
			},
			gvks: map[schema.GroupVersionKind]bool{
				{"pipecd.dev", "v1beta1", "Foo"}:  true,
				{"pipecd.dev", "v1alpha1", "Foo"}: true,
				{"pipecd.dev", "v1alpha1", "Bar"}: false,
			},
		},
		{
			name: "exclude config",
			cfg: config.KubernetesAppStateInformer{
				ExcludeResources: []config.KubernetesResourceMatcher{
					{APIVersion: "networking.k8s.io/v1"},
					{APIVersion: "apps/v1", Kind: "Deployment"},
				},
			},
			gvks: map[schema.GroupVersionKind]bool{
				{"apps", "v1", "ReplicaSet"}:           true,
				{"apps", "v1", "Deployment"}:           false,
				{"networking.k8s.io", "v1", "Ingress"}: false,
			},
		},
	}

	for _, tc := range testcases {
		m := newResourceMatcher(tc.cfg)
		for gvk, expected := range tc.gvks {
			desc := fmt.Sprintf("%s: %v", tc.name, gvk)
			gvk, expected := gvk, expected
			t.Run(desc, func(t *testing.T) {
				t.Parallel()

				matched := m.Match(gvk)
				assert.Equal(t, expected, matched)
			})
		}
	}
}

func TestReflector_onObjectDelete(t *testing.T) {
	t.Parallel()

	obj := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod"}}
	obj.SetName("pod")
	obj.SetUID("uid")
	other := obj.DeepCopy()
	other.SetAnnotations(map[string]string{provider.LabelPiped: "other"})
	for _, tt := range []struct {
		name string
		obj  any
		want bool
	}{
		{"direct", obj, true},
		{"tombstone", cache.DeletedFinalStateUnknown{Key: "pod", Obj: obj}, true},
		{"other piped", other, false},
		{"other piped tombstone", cache.DeletedFinalStateUnknown{Key: "pod", Obj: other}, false},
		{"missing object", cache.DeletedFinalStateUnknown{Key: "pod"}, false},
		{"invalid tombstone", cache.DeletedFinalStateUnknown{Key: "pod", Obj: 1}, false},
		{"invalid object", 1, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var deleted []*unstructured.Unstructured
			r := reflector{
				pipedConfig: &config.PipedSpec{PipedID: "piped"},
				logger:      zap.NewNop(),
				onDelete: func(u *unstructured.Unstructured) {
					deleted = append(deleted, u)
				},
			}
			assert.NotPanics(t, func() { r.onObjectDelete(tt.obj) })
			if tt.want {
				assert.Equal(t, []*unstructured.Unstructured{obj}, deleted)
			} else {
				assert.Empty(t, deleted)
			}
		})
	}
}
