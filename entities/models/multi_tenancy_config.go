//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"context"
	"encoding/json"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// MultiTenancyConfig Configuration related to multi-tenancy within a class
//
// swagger:model MultiTenancyConfig
type MultiTenancyConfig struct {

	// Whether or not multi-tenancy is enabled for this class
	Enabled bool `json:"enabled,omitempty"`

	// The class property which is used to separate tenants
	// Enum: [uuid text]
	TenantKey string `json:"tenantKey,omitempty"`
}

// Validate validates this multi tenancy config
func (m *MultiTenancyConfig) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateTenantKey(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

var multiTenancyConfigTypeTenantKeyPropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["uuid","text"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		multiTenancyConfigTypeTenantKeyPropEnum = append(multiTenancyConfigTypeTenantKeyPropEnum, v)
	}
}

const (

	// MultiTenancyConfigTenantKeyUUID captures enum value "uuid"
	MultiTenancyConfigTenantKeyUUID string = "uuid"

	// MultiTenancyConfigTenantKeyText captures enum value "text"
	MultiTenancyConfigTenantKeyText string = "text"
)

// prop value enum
func (m *MultiTenancyConfig) validateTenantKeyEnum(path, location string, value string) error {
	if err := validate.EnumCase(path, location, value, multiTenancyConfigTypeTenantKeyPropEnum, true); err != nil {
		return err
	}
	return nil
}

func (m *MultiTenancyConfig) validateTenantKey(formats strfmt.Registry) error {
	if swag.IsZero(m.TenantKey) { // not required
		return nil
	}

	// value enum
	if err := m.validateTenantKeyEnum("tenantKey", "body", m.TenantKey); err != nil {
		return err
	}

	return nil
}

// ContextValidate validates this multi tenancy config based on context it is used
func (m *MultiTenancyConfig) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *MultiTenancyConfig) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *MultiTenancyConfig) UnmarshalBinary(b []byte) error {
	var res MultiTenancyConfig
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}