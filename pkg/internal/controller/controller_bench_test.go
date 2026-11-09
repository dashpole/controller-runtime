/*
Copyright 2026 The Kubernetes Authors.

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

package controller

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"

	"sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func BenchmarkReconcileHandler_TracingDisabled(b *testing.B) {
	q := priorityqueue.New[reconcile.Request]("bench-controller")
	defer q.ShutDown()

	c := New(Options[reconcile.Request]{
		Name:                    "bench-controller",
		MaxConcurrentReconciles: 1,
		Do: reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
			return reconcile.Result{}, nil
		}),
		NewQueue: func(string, workqueue.TypedRateLimiter[reconcile.Request]) workqueue.TypedRateLimitingInterface[reconcile.Request] {
			return q
		},
		LogConstructor: func(*reconcile.Request) logr.Logger {
			return logr.Discard()
		},
	})
	c.Queue = q

	ctx := context.Background()
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: "default",
			Name:      "test-obj",
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		c.reconcileHandler(ctx, req, 0)
	}
}

func BenchmarkReconcileHandler_TracingEnabled(b *testing.B) {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	q := priorityqueue.New[reconcile.Request]("bench-controller-traced")
	defer q.ShutDown()

	gvk := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	c := New(Options[reconcile.Request]{
		Name:                    "bench-controller-traced",
		MaxConcurrentReconciles: 1,
		TracerProvider:          tp,
		TraceMetadataExtractor: func(_ context.Context, req reconcile.Request) (ObjectTraceMetadata, error) {
			return ObjectTraceMetadata{
				GVK:       gvk,
				Namespace: req.Namespace,
				Name:      req.Name,
			}, nil
		},
		Do: reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
			return reconcile.Result{}, nil
		}),
		NewQueue: func(string, workqueue.TypedRateLimiter[reconcile.Request]) workqueue.TypedRateLimitingInterface[reconcile.Request] {
			return q
		},
		LogConstructor: func(*reconcile.Request) logr.Logger {
			return logr.Discard()
		},
	})
	c.Queue = q

	ctx := context.Background()
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: "default",
			Name:      "test-obj",
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		c.reconcileHandler(ctx, req, 0)
	}
}
