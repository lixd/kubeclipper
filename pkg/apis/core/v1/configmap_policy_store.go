package v1

import (
	"context"
	"encoding/json"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	modelscore "github.com/kubeclipper/kubeclipper/pkg/models/core"
	schemecorev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func DeliveryPolicyConfigMapNameForTest() string {
	return deliveryapis.DeliveryPolicyConfigMapName
}

func DeliveryPolicyConfigMapKeyForTest() string {
	return deliveryapis.DeliveryPolicyConfigMapKey
}

type configMapPolicyStore struct {
	coreOperator modelscore.Operator
}

func newConfigMapPolicyStore(coreOperator modelscore.Operator) deliveryapis.PolicyStore {
	if coreOperator == nil {
		return nil
	}
	return &configMapPolicyStore{coreOperator: coreOperator}
}

func (s *configMapPolicyStore) Get(ctx context.Context) (*deliveryapis.SupportPolicy, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cm, err := s.coreOperator.GetConfigMap(ctx, deliveryapis.DeliveryPolicyConfigMapName)
	if err != nil {
		return nil, err
	}
	policyData, ok := cm.Data[deliveryapis.DeliveryPolicyConfigMapKey]
	if !ok || policyData == "" {
		return nil, fmt.Errorf("configmap %s does not contain %s", deliveryapis.DeliveryPolicyConfigMapName, deliveryapis.DeliveryPolicyConfigMapKey)
	}
	var policy deliveryapis.SupportPolicy
	if err = deliveryapis.DecodeSupportPolicy([]byte(policyData), &policy); err != nil {
		return nil, err
	}
	if err = policy.Validate(); err != nil {
		return nil, err
	}
	return &policy, nil
}

func (s *configMapPolicyStore) Update(ctx context.Context, mutator func(*deliveryapis.SupportPolicy) error) error {
	if mutator == nil {
		return fmt.Errorf("policy mutator is nil")
	}
	policy := deliveryapis.NewSupportPolicy("default")
	var configMap *schemecorev1.ConfigMap
	hadExisting := false
	existingPolicy, err := s.Get(ctx)
	switch {
	case err == nil:
		policy = existingPolicy
		hadExisting = true
		configMap, err = s.coreOperator.GetConfigMap(ctx, deliveryapis.DeliveryPolicyConfigMapName)
		if err != nil {
			return err
		}
	case !apierrors.IsNotFound(err):
		return err
	}
	if err = mutator(policy); err != nil {
		return err
	}
	if err = policy.Validate(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(policy, "", "  ")
	if err != nil {
		return err
	}
	if configMap == nil {
		configMap = &schemecorev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: deliveryapis.DeliveryPolicyConfigMapName},
			Data:       map[string]string{},
		}
	}
	if configMap.Data == nil {
		configMap.Data = map[string]string{}
	}
	configMap.Data[deliveryapis.DeliveryPolicyConfigMapKey] = string(data)
	if !hadExisting {
		_, err = s.coreOperator.CreateConfigMap(ctx, configMap)
		return err
	}
	_, err = s.coreOperator.UpdateConfigMap(ctx, configMap)
	return err
}
