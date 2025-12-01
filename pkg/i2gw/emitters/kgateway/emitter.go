/*
Copyright 2024 The Kubernetes Authors.
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

package kgateway

import (
	"fmt"

	"github.com/kubernetes-sigs/ingress2gateway/pkg/i2gw"
	"github.com/kubernetes-sigs/ingress2gateway/pkg/i2gw/emitter_intermediate"
	"github.com/kubernetes-sigs/ingress2gateway/pkg/i2gw/emitters/common"

	kgwv1a1 "github.com/kgateway-dev/kgateway/v2/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
)

const (
	Name = "kgateway"
)

// Emitter implements the Emitter interface.
type Emitter struct{}

// Ensure Emitter satisfies the Emitter interface.
var _ i2gw.Emitter = &Emitter{}

func init() {
	i2gw.EmitterConstructorByName[Name] = newEmitter
}

// newEmitter returns a new instance of the Kgateway Emitter.
func newEmitter(_ *i2gw.EmitterConf) i2gw.Emitter {
	return &Emitter{}
}

type emittedResources struct {
	trafficPolicies map[types.NamespacedName]*kgwv1a1.TrafficPolicy
}

func (e *Emitter) ToGatewayResources(ir emitter_intermediate.IR) (i2gw.GatewayResources, field.ErrorList) {
	gatewayResources, errorList := common.ToGatewayResources(ir)
	if len(errorList) > 0 {
		return gatewayResources, errorList
	}
	errorList = e.toProviderResources(ir, &gatewayResources)

	return gatewayResources, errorList
}

func (e *Emitter) toProviderResources(ir emitter_intermediate.IR, gwResources *i2gw.GatewayResources) field.ErrorList {
	emittedResources := &emittedResources{
		trafficPolicies: make(map[types.NamespacedName]*kgwv1a1.TrafficPolicy),
	}
	errorList := e.bufferToProviderResource(emittedResources, ir)

	for k, tp := range emittedResources.trafficPolicies {
		obj, err := i2gw.CastToUnstructured(tp)
		if err != nil {
			errorList = append(errorList, field.InternalError(field.NewPath("trafficpolicy", k.Namespace, k.Name),
				fmt.Errorf("failed to convert Kgateway TrafficPolicy to unstructured: %w", err),
			))
			continue
		}
		gwResources.GatewayExtensions = append(gwResources.GatewayExtensions, *obj)
	}
	return errorList
}

func (e *Emitter) bufferToProviderResource(emittedResources *emittedResources, ir emitter_intermediate.IR) field.ErrorList {
	var errs field.ErrorList

	for httpRouteKey, httpRouteContext := range ir.HTTPRoutes {
		settings := httpRouteContext.ExtensionSettings
		if len(settings) == 0 {
			continue
		}

		allAttachedKey := intermediate.RouteSettingsAttachment{
			Rule:    intermediate.IndexAttachAllRules,
			Backend: intermediate.IndexAttachAllBackends,
		}

		// Check if the settings apply to all rules
		if setting, ok := settings[allAttachedKey]; ok && setting.Buffer != nil {
			btpNN := types.NamespacedName{
				Name:      httpRouteKey.Name,
				Namespace: httpRouteKey.Namespace,
			}

			btp, exists := emittedResources.backendTrafficPolicies[btpNN]
			if !exists {
				btp = &egv1a1.BackendTrafficPolicy{
					ObjectMeta: metav1.ObjectMeta{
						Name:      btpNN.Name,
						Namespace: btpNN.Namespace,
						Annotations: map[string]string{
							i2gw.GeneratorAnnotationKey: fmt.Sprintf("ingress2gateway-%s", i2gw.Version),
						},
					},
					Spec: egv1a1.BackendTrafficPolicySpec{
						PolicyTargetReferences: egv1a1.PolicyTargetReferences{
							TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{
								{
									LocalPolicyTargetReference: gatewayv1.LocalPolicyTargetReference{
										Group: gatewayv1.Group(gvk.HTTPRouteGVK.Group),
										Kind:  gatewayv1.Kind(gvk.HTTPRouteGVK.Kind),
										Name:  gatewayv1.ObjectName(httpRouteKey.Name),
									},
								},
							},
						},
					},
				}
				btp.SetGroupVersionKind(BackendTrafficPolicyGVK)
				emittedResources.backendTrafficPolicies[btpNN] = btp
			}

			btp.Spec.RequestBuffer = &egv1a1.RequestBuffer{
				Limit: *setting.Buffer,
			}

			// Mark the setting as processed
			if setting.ProcessingStatus == nil {
				setting.ProcessingStatus = make(map[intermediate.ExtensionFeature]*intermediate.ExtensionSettingMetadata)
			}
			status, ok := setting.ProcessingStatus[intermediate.ExtensionFeatureBodyBuffer]
			if !ok {
				status = &intermediate.ExtensionSettingMetadata{}
				setting.ProcessingStatus[intermediate.ExtensionFeatureBodyBuffer] = status
			}
			status.Emitter = Name
			status.Attached = true

			notify(notifications.InfoNotification,
				fmt.Sprintf("generated BackendTrafficPolicy with buffer size %s for HTTPRoute %s/%s",
					setting.Buffer.String(), httpRouteKey.Namespace, httpRouteKey.Name),
				btp)
		} else {
			// TODO [kkk777-7]: Should handle per route rule name (sectionName) attachments.
			// This would enable per-rule buffer configuration in a merged HTTPRoute, even when
			// multiple Ingresses with different buffer sizes are consolidated into a single HTTPRoute.
			// We can't support this until common HTTPRoute output supports route rule names.

			// Check if there are per-rule settings that we don't support yet
			for attachment, setting := range settings {
				if setting.Buffer != nil {
					notify(notifications.WarningNotification,
						fmt.Sprintf("per-rule buffer configuration is not supported yet for HTTPRoute %s/%s (rule %d), skipping",
							httpRouteKey.Namespace, httpRouteKey.Name, attachment.Rule),
						&httpRouteContext.HTTPRoute)
					break
				}
			}
		}
	}
	return errs
}

// Emit consumes the emitter IR and returns Kgateway-specific resources.
func (e *Emitter) Emit(ir emitter_intermediate.IR) (i2gw.GatewayResources, field.ErrorList) {
	var out []client.Object
	var errs field.ErrorList

	for httpRouteKey, httpRouteContext := range ir.HTTPRoutes {
		tp := map[string]*kgwv1a1.TrafficPolicy{}
		createIfNeeded := func(name string) {
			if tp[name] == nil {
				tp[name] = &kgwv1a1.TrafficPolicy{
					ObjectMeta: metav1.ObjectMeta{
						Name:      name,
						Namespace: httpRouteKey.Namespace,
					},
					Spec: kgwv1a1.TrafficPolicySpec{},
				}
				tp[name].SetGroupVersionKind(TrafficPolicyGVK)
			}
		}

		for polSourceIngressName, pol := range ingx.Policies {
			var t *kgwv1a1.TrafficPolicy
			if pol.Buffer != nil {
				createIfNeeded(polSourceIngressName)
				t = tp[polSourceIngressName]
				t.Spec.Buffer = &kgwv1a1.Buffer{
					MaxRequestSize: pol.Buffer,
				}
			}
			if t == nil {
				continue
			}

			if len(pol.RuleBackendSources) == numRules(httpRouteContext.HTTPRoute) {
				// Full coverage via targetRefs.
				t.Spec.TargetRefs = []kgwv1a1.LocalPolicyTargetReferenceWithSectionName{{
					LocalPolicyTargetReference: kgwv1a1.LocalPolicyTargetReference{
						Name: gwv1.ObjectName(httpRouteKey.Name),
					},
				}}
			} else {
				// Partial coverage via ExtensionRef filters on backendRefs.
				for _, idx := range pol.RuleBackendSources {
					httpRouteContext.Spec.Rules[idx.Rule].BackendRefs[idx.Backend].Filters =
						append(
							httpRouteContext.Spec.Rules[idx.Rule].BackendRefs[idx.Backend].Filters,
							gwv1.HTTPRouteFilter{
								Type: gwv1.HTTPRouteFilterExtensionRef,
								ExtensionRef: &gwv1.LocalObjectReference{
									Group: gwv1.Group(TrafficPolicyGVK.Group),
									Kind:  gwv1.Kind(TrafficPolicyGVK.Kind),
									Name:  gwv1.ObjectName(t.Name),
								},
							},
						)
				}
			}
		}

		// Write back the mutated HTTPRouteContext into the IR.
		ir.HTTPRoutes[httpRouteKey] = httpRouteContext

		// Collect TrafficPolicies.
		for _, tp := range tp {
			out = append(out, tp)
		}
	}

	if len(errs) > 0 {
		return out, i2gw.AggregatedErrs(errs)
	}
	return out, nil
}

func numRules(hr gwv1.HTTPRoute) int {
	n := 0
	for _, r := range hr.Spec.Rules {
		n += len(r.BackendRefs)
	}
	return n
}
