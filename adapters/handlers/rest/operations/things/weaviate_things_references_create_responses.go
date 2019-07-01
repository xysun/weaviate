/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
 * LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
 * CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */ // Code generated by go-swagger; DO NOT EDIT.

package things

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	models "github.com/semi-technologies/weaviate/entities/models"
)

// WeaviateThingsReferencesCreateOKCode is the HTTP code returned for type WeaviateThingsReferencesCreateOK
const WeaviateThingsReferencesCreateOKCode int = 200

/*WeaviateThingsReferencesCreateOK Successfully added the reference.

swagger:response weaviateThingsReferencesCreateOK
*/
type WeaviateThingsReferencesCreateOK struct {
}

// NewWeaviateThingsReferencesCreateOK creates WeaviateThingsReferencesCreateOK with default headers values
func NewWeaviateThingsReferencesCreateOK() *WeaviateThingsReferencesCreateOK {

	return &WeaviateThingsReferencesCreateOK{}
}

// WriteResponse to the client
func (o *WeaviateThingsReferencesCreateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(200)
}

// WeaviateThingsReferencesCreateUnauthorizedCode is the HTTP code returned for type WeaviateThingsReferencesCreateUnauthorized
const WeaviateThingsReferencesCreateUnauthorizedCode int = 401

/*WeaviateThingsReferencesCreateUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateThingsReferencesCreateUnauthorized
*/
type WeaviateThingsReferencesCreateUnauthorized struct {
}

// NewWeaviateThingsReferencesCreateUnauthorized creates WeaviateThingsReferencesCreateUnauthorized with default headers values
func NewWeaviateThingsReferencesCreateUnauthorized() *WeaviateThingsReferencesCreateUnauthorized {

	return &WeaviateThingsReferencesCreateUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateThingsReferencesCreateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// WeaviateThingsReferencesCreateForbiddenCode is the HTTP code returned for type WeaviateThingsReferencesCreateForbidden
const WeaviateThingsReferencesCreateForbiddenCode int = 403

/*WeaviateThingsReferencesCreateForbidden Forbidden

swagger:response weaviateThingsReferencesCreateForbidden
*/
type WeaviateThingsReferencesCreateForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewWeaviateThingsReferencesCreateForbidden creates WeaviateThingsReferencesCreateForbidden with default headers values
func NewWeaviateThingsReferencesCreateForbidden() *WeaviateThingsReferencesCreateForbidden {

	return &WeaviateThingsReferencesCreateForbidden{}
}

// WithPayload adds the payload to the weaviate things references create forbidden response
func (o *WeaviateThingsReferencesCreateForbidden) WithPayload(payload *models.ErrorResponse) *WeaviateThingsReferencesCreateForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate things references create forbidden response
func (o *WeaviateThingsReferencesCreateForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingsReferencesCreateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateThingsReferencesCreateUnprocessableEntityCode is the HTTP code returned for type WeaviateThingsReferencesCreateUnprocessableEntity
const WeaviateThingsReferencesCreateUnprocessableEntityCode int = 422

/*WeaviateThingsReferencesCreateUnprocessableEntity Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the property exists or that it is a class?

swagger:response weaviateThingsReferencesCreateUnprocessableEntity
*/
type WeaviateThingsReferencesCreateUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewWeaviateThingsReferencesCreateUnprocessableEntity creates WeaviateThingsReferencesCreateUnprocessableEntity with default headers values
func NewWeaviateThingsReferencesCreateUnprocessableEntity() *WeaviateThingsReferencesCreateUnprocessableEntity {

	return &WeaviateThingsReferencesCreateUnprocessableEntity{}
}

// WithPayload adds the payload to the weaviate things references create unprocessable entity response
func (o *WeaviateThingsReferencesCreateUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *WeaviateThingsReferencesCreateUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate things references create unprocessable entity response
func (o *WeaviateThingsReferencesCreateUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingsReferencesCreateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateThingsReferencesCreateInternalServerErrorCode is the HTTP code returned for type WeaviateThingsReferencesCreateInternalServerError
const WeaviateThingsReferencesCreateInternalServerErrorCode int = 500

/*WeaviateThingsReferencesCreateInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response weaviateThingsReferencesCreateInternalServerError
*/
type WeaviateThingsReferencesCreateInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewWeaviateThingsReferencesCreateInternalServerError creates WeaviateThingsReferencesCreateInternalServerError with default headers values
func NewWeaviateThingsReferencesCreateInternalServerError() *WeaviateThingsReferencesCreateInternalServerError {

	return &WeaviateThingsReferencesCreateInternalServerError{}
}

// WithPayload adds the payload to the weaviate things references create internal server error response
func (o *WeaviateThingsReferencesCreateInternalServerError) WithPayload(payload *models.ErrorResponse) *WeaviateThingsReferencesCreateInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate things references create internal server error response
func (o *WeaviateThingsReferencesCreateInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingsReferencesCreateInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
