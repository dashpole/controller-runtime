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
	"sigs.k8s.io/controller-runtime/pkg/internal/controller"
)

const (
	// TraceparentAnnotationKey is the standard prefixed Kubernetes annotation key for W3C traceparent.
	TraceparentAnnotationKey = controller.TraceparentAnnotationKey
	// TracestateAnnotationKey is the standard prefixed Kubernetes annotation key for W3C tracestate.
	TracestateAnnotationKey = controller.TracestateAnnotationKey

	// InstrumentationName is the OpenTelemetry instrumentation scope name for controller-runtime.
	InstrumentationName = controller.InstrumentationName

	// TriggerTypeMutation indicates reconciliation was triggered by a resource spec/metadata mutation.
	TriggerTypeMutation = controller.TriggerTypeMutation
	// TriggerTypePeriodicResync indicates reconciliation was triggered by an initial list or periodic cache resync.
	TriggerTypePeriodicResync = controller.TriggerTypePeriodicResync
	// TriggerTypeRequeue indicates reconciliation was triggered by a workqueue requeue (error, Requeue, or RequeueAfter).
	TriggerTypeRequeue = controller.TriggerTypeRequeue
)

// ObjectCarrier implements OpenTelemetry's propagation.TextMapCarrier for Kubernetes objects,
// mapping standard W3C "traceparent" and "tracestate" keys to their prefixed Kubernetes annotations.
type ObjectCarrier = controller.ObjectCarrier

// NewObjectCarrier returns an ObjectCarrier backed by the given metav1.Object's annotations.
var NewObjectCarrier = controller.NewObjectCarrier

// ObjectTraceMetadata holds trace context and resource metadata extracted from the
// reconciled object in the local informer cache prior to invoking Reconcile.
type ObjectTraceMetadata = controller.ObjectTraceMetadata

// TraceMetadataExtractor extracts trace context and object metadata for a queued request.
type TraceMetadataExtractor[request comparable] = controller.TraceMetadataExtractor[request]

// NewCacheTraceMetadataExtractor returns a TraceMetadataExtractor for reconcile.Request that
// reads the target object from the given client.Reader (typically the Manager's local informer cache)
// using a shallow copy of prototype and extracts W3C trace context via ObjectCarrier.
var NewCacheTraceMetadataExtractor = controller.NewCacheTraceMetadataExtractor

// IsNoopTracerProvider reports whether tp is nil or OpenTelemetry's no-op TracerProvider.
var IsNoopTracerProvider = controller.IsNoopTracerProvider
