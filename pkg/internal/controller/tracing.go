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
	"errors"
	"reflect"
	"sort"
	"strings"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// TraceparentAnnotationKey is the standard prefixed Kubernetes annotation key for W3C traceparent.
	TraceparentAnnotationKey = "tracing.k8s.io/traceparent"
	// TracestateAnnotationKey is the standard prefixed Kubernetes annotation key for W3C tracestate.
	TracestateAnnotationKey = "tracing.k8s.io/tracestate"

	w3cTraceparentKey = "traceparent"
	w3cTracestateKey  = "tracestate"

	// InstrumentationName is the OpenTelemetry instrumentation scope name for controller-runtime.
	InstrumentationName = "sigs.k8s.io/controller-runtime"

	// TriggerTypeMutation indicates reconciliation was triggered by a resource spec/metadata mutation.
	TriggerTypeMutation = "mutation"
	// TriggerTypePeriodicResync indicates reconciliation was triggered by an initial list or periodic cache resync.
	TriggerTypePeriodicResync = "periodic_resync"
	// TriggerTypeRequeue indicates reconciliation was triggered by a workqueue requeue (error, Requeue, or RequeueAfter).
	TriggerTypeRequeue = "requeue"

	attrControllerName          = "k8s.controller.name"
	attrControllerTriggerType   = "k8s.controller.trigger.type"
	attrControllerResult        = "k8s.controller.result"
	attrControllerRequeueAfter  = "k8s.controller.requeue_after"
	attrControllerTerminalError = "k8s.controller.terminal_error"
	attrResourceGroup           = "k8s.resource.group"
	attrResourceVersion         = "k8s.resource.version"
	attrResourceKind            = "k8s.resource.kind"
	attrResourceNamespace       = "k8s.resource.namespace"
	attrResourceName            = "k8s.resource.name"
	attrResourceUID             = "k8s.resource.uid"
	attrResourceGeneration      = "k8s.resource.generation"
)

var (
	_ propagation.TextMapCarrier = ObjectCarrier{}

	noopTracerProviderType = reflect.TypeOf(noop.NewTracerProvider())
)

// IsNoopTracerProvider reports whether tp is nil or OpenTelemetry's no-op TracerProvider.
func IsNoopTracerProvider(tp trace.TracerProvider) bool {
	if tp == nil {
		return true
	}
	return reflect.TypeOf(tp) == noopTracerProviderType
}

// ObjectCarrier implements OpenTelemetry's propagation.TextMapCarrier for Kubernetes objects,
// mapping standard W3C "traceparent" and "tracestate" keys to their prefixed Kubernetes annotations.
type ObjectCarrier struct {
	obj metav1.Object
}

// NewObjectCarrier returns an ObjectCarrier backed by the given metav1.Object's annotations.
func NewObjectCarrier(obj metav1.Object) ObjectCarrier {
	return ObjectCarrier{obj: obj}
}

func isNilObject(obj metav1.Object) bool {
	if obj == nil {
		return true
	}
	v := reflect.ValueOf(obj)
	return v.Kind() == reflect.Pointer && v.IsNil()
}

func mapCarrierKeyToAnnotation(key string) string {
	switch strings.ToLower(key) {
	case w3cTraceparentKey:
		return TraceparentAnnotationKey
	case w3cTracestateKey:
		return TracestateAnnotationKey
	default:
		return key
	}
}

func mapAnnotationToCarrierKey(key string) string {
	switch key {
	case TraceparentAnnotationKey:
		return w3cTraceparentKey
	case TracestateAnnotationKey:
		return w3cTracestateKey
	default:
		return key
	}
}

// Get returns the value associated with the passed key from the object's annotations.
func (c ObjectCarrier) Get(key string) string {
	if isNilObject(c.obj) {
		return ""
	}
	ann := c.obj.GetAnnotations()
	if len(ann) == 0 {
		return ""
	}
	return ann[mapCarrierKeyToAnnotation(key)]
}

// Set stores the key-value pair on the object's annotations.
func (c ObjectCarrier) Set(key string, val string) {
	if isNilObject(c.obj) || val == "" {
		return
	}
	ann := c.obj.GetAnnotations()
	if ann == nil {
		ann = make(map[string]string, 1)
	}
	ann[mapCarrierKeyToAnnotation(key)] = val
	c.obj.SetAnnotations(ann)
}

// Keys lists the keys stored in this carrier.
func (c ObjectCarrier) Keys() []string {
	if isNilObject(c.obj) {
		return nil
	}
	ann := c.obj.GetAnnotations()
	if len(ann) == 0 {
		return nil
	}
	keys := make([]string, 0, len(ann))
	for k := range ann {
		keys = append(keys, mapAnnotationToCarrierKey(k))
	}
	sort.Strings(keys)
	return keys
}

// ObjectTraceMetadata holds trace context and resource metadata extracted from the
// reconciled object in the local informer cache prior to invoking Reconcile.
type ObjectTraceMetadata struct {
	SpanContext trace.SpanContext
	GVK         schema.GroupVersionKind
	Namespace   string
	Name        string
	UID         types.UID
	Generation  int64
}

// TraceMetadataExtractor extracts trace context and object metadata for a queued request.
type TraceMetadataExtractor[request comparable] func(ctx context.Context, req request) (ObjectTraceMetadata, error)

// requeueState tracks whether a re-enqueued item originated from a resource mutation
// and the object metadata observed on that pass, so periodic resync requeues do not attach
// stale Span Links while superseding watch mutations (including deletes and spec updates) are detected.
type requeueState struct {
	fromMutation bool
	spanContext  trace.SpanContext
	uid          types.UID
	generation   int64
}

// NewCacheTraceMetadataExtractor returns a TraceMetadataExtractor for reconcile.Request that
// reads the target object from the given client.Reader (typically the Manager's local informer cache)
// using a shallow copy of prototype and extracts W3C trace context via ObjectCarrier.
func NewCacheTraceMetadataExtractor(
	reader client.Reader,
	prototype client.Object,
	gvk schema.GroupVersionKind,
	propagators propagation.TextMapPropagator,
) TraceMetadataExtractor[reconcile.Request] {
	prop := resolvePropagator(propagators)
	if !isNilObject(prototype) {
		if gvk.Empty() {
			gvk = prototype.GetObjectKind().GroupVersionKind()
		} else if prototype.GetObjectKind().GroupVersionKind().Empty() {
			if cloned, ok := prototype.DeepCopyObject().(client.Object); ok {
				cloned.GetObjectKind().SetGroupVersionKind(gvk)
				prototype = cloned
			}
		}
	}
	return func(ctx context.Context, req reconcile.Request) (ObjectTraceMetadata, error) {
		meta := ObjectTraceMetadata{
			GVK:       gvk,
			Namespace: req.Namespace,
			Name:      req.Name,
		}
		if reader == nil || isNilObject(prototype) {
			return meta, nil
		}
		cloned, ok := prototype.DeepCopyObject().(client.Object)
		if !ok {
			return meta, nil
		}
		if err := reader.Get(ctx, req.NamespacedName, cloned, client.UnsafeDisableDeepCopy); err != nil {
			if apierrors.IsNotFound(err) {
				return meta, nil
			}
			return meta, err
		}
		populateFromObject(&meta, cloned, prop)
		return meta, nil
	}
}

func resolvePropagator(propagators propagation.TextMapPropagator) propagation.TextMapPropagator {
	if propagators != nil {
		return propagators
	}
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

func populateFromObject(meta *ObjectTraceMetadata, obj metav1.Object, prop propagation.TextMapPropagator) {
	if isNilObject(obj) {
		return
	}
	if meta.Namespace == "" {
		meta.Namespace = obj.GetNamespace()
	}
	if meta.Name == "" {
		meta.Name = obj.GetName()
	}
	meta.UID = obj.GetUID()
	meta.Generation = obj.GetGeneration()
	if meta.GVK.Empty() {
		if rtObj, ok := obj.(runtime.Object); ok {
			meta.GVK = rtObj.GetObjectKind().GroupVersionKind()
		}
	}
	extractedCtx := prop.Extract(context.Background(), NewObjectCarrier(obj))
	meta.SpanContext = trace.SpanContextFromContext(extractedCtx)
}

func (c *Controller[request]) getTracer() trace.Tracer {
	if c.tracer != nil {
		return c.tracer
	}
	return c.TracerProvider.Tracer(InstrumentationName)
}

func (c *Controller[request]) extractMetadata(ctx context.Context, req request, log logr.Logger) ObjectTraceMetadata {
	if c.TraceMetadataExtractor != nil {
		meta, err := c.TraceMetadataExtractor(ctx, req)
		if err != nil {
			log.V(5).Info("Failed to extract trace metadata", "err", err)
		}
		if r, ok := any(req).(reconcile.Request); ok {
			if meta.Namespace == "" {
				meta.Namespace = r.Namespace
			}
			if meta.Name == "" {
				meta.Name = r.Name
			}
		}
		return meta
	}

	var meta ObjectTraceMetadata
	if r, ok := any(req).(reconcile.Request); ok {
		meta.Namespace = r.Namespace
		meta.Name = r.Name
		return meta
	}

	if obj, ok := any(req).(metav1.Object); ok && !isNilObject(obj) {
		populateFromObject(&meta, obj, resolvePropagator(c.Propagators))
	}
	return meta
}

func (c *Controller[request]) startReconcileSpan(
	ctx context.Context,
	req request,
	priority int,
	log logr.Logger,
) (context.Context, logr.Logger, trace.Span, requeueState) {
	meta := c.extractMetadata(ctx, req, log)
	prevVal, wasRequeued := c.requeuedKeys.LoadAndDelete(req)
	prevState, _ := prevVal.(requeueState)

	var triggerType string
	var fromMutation bool
	switch {
	case wasRequeued:
		supersedingMutation := (!prevState.fromMutation && priority > handler.LowPriority) ||
			(meta.SpanContext.IsValid() && !meta.SpanContext.Equal(prevState.spanContext)) ||
			(prevState.uid != "" && meta.UID != prevState.uid) ||
			(prevState.generation > 0 && meta.Generation > 0 && meta.Generation != prevState.generation)
		if supersedingMutation {
			triggerType = TriggerTypeMutation
			fromMutation = true
		} else {
			triggerType = TriggerTypeRequeue
			fromMutation = prevState.fromMutation
		}
	case priority == handler.LowPriority:
		triggerType = TriggerTypePeriodicResync
		fromMutation = false
	default:
		triggerType = TriggerTypeMutation
		fromMutation = true
	}

	attrs := make([]attribute.KeyValue, 0, 9)
	if c.Name != "" {
		attrs = append(attrs, attribute.String(attrControllerName, c.Name))
	}
	if !meta.GVK.Empty() {
		attrs = append(attrs,
			attribute.String(attrResourceGroup, meta.GVK.Group),
			attribute.String(attrResourceVersion, meta.GVK.Version),
			attribute.String(attrResourceKind, meta.GVK.Kind),
		)
	}
	if meta.Namespace != "" {
		attrs = append(attrs, attribute.String(attrResourceNamespace, meta.Namespace))
	}
	if meta.Name != "" {
		attrs = append(attrs, attribute.String(attrResourceName, meta.Name))
	}
	if meta.UID != "" {
		attrs = append(attrs, attribute.String(attrResourceUID, string(meta.UID)))
	}
	if meta.Generation > 0 {
		attrs = append(attrs, attribute.Int64(attrResourceGeneration, meta.Generation))
	}
	attrs = append(attrs, attribute.String(attrControllerTriggerType, triggerType))

	startOpts := make([]trace.SpanStartOption, 0, 4)
	startOpts = append(startOpts,
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attrs...),
	)
	if fromMutation && meta.SpanContext.IsValid() {
		startOpts = append(startOpts, trace.WithLinks(trace.Link{SpanContext: meta.SpanContext}))
	}

	spanName := "Reconcile"
	if meta.GVK.Kind != "" {
		spanName = "Reconcile" + meta.GVK.Kind
	} else if c.Name != "" {
		spanName = "Reconcile " + c.Name
	}

	ctx, span := c.getTracer().Start(ctx, spanName, startOpts...)
	if sc := span.SpanContext(); sc.IsValid() {
		log = log.WithValues(
			"trace_id", sc.TraceID().String(),
			"span_id", sc.SpanID().String(),
		)
	}
	return ctx, log, span, requeueState{
		fromMutation: fromMutation,
		spanContext:  meta.SpanContext,
		uid:          meta.UID,
		generation:   meta.Generation,
	}
}

func (c *Controller[request]) endReconcileSpan(
	span trace.Span,
	req request,
	result reconcile.Result,
	err error,
	requeueOnErr bool,
	state requeueState,
) {
	switch {
	case err != nil:
		isTerminal := errors.Is(err, reconcile.TerminalError(nil))
		if !isTerminal && requeueOnErr {
			c.requeuedKeys.Store(req, state)
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		if isTerminal {
			span.SetAttributes(
				attribute.String(attrControllerResult, labelError),
				attribute.Bool(attrControllerTerminalError, true),
			)
		} else {
			span.SetAttributes(attribute.String(attrControllerResult, labelError))
		}
	case result.RequeueAfter > 0:
		c.requeuedKeys.Store(req, state)
		span.SetAttributes(
			attribute.String(attrControllerResult, labelRequeueAfter),
			attribute.Float64(attrControllerRequeueAfter, result.RequeueAfter.Seconds()),
		)
		span.SetStatus(codes.Ok, "")
	case result.Requeue: //nolint:staticcheck // We have to handle it until it is removed
		c.requeuedKeys.Store(req, state)
		span.SetAttributes(attribute.String(attrControllerResult, labelRequeue))
		span.SetStatus(codes.Ok, "")
	default:
		span.SetAttributes(attribute.String(attrControllerResult, labelSuccess))
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}
