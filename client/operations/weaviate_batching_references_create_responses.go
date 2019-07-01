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

package operations

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"

	strfmt "github.com/go-openapi/strfmt"

	models "github.com/semi-technologies/weaviate/entities/models"
)

// WeaviateBatchingReferencesCreateReader is a Reader for the WeaviateBatchingReferencesCreate structure.
type WeaviateBatchingReferencesCreateReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *WeaviateBatchingReferencesCreateReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {

	case 200:
		result := NewWeaviateBatchingReferencesCreateOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil

	case 401:
		result := NewWeaviateBatchingReferencesCreateUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	case 403:
		result := NewWeaviateBatchingReferencesCreateForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	case 422:
		result := NewWeaviateBatchingReferencesCreateUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	case 500:
		result := NewWeaviateBatchingReferencesCreateInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	default:
		return nil, runtime.NewAPIError("unknown error", response, response.Code())
	}
}

// NewWeaviateBatchingReferencesCreateOK creates a WeaviateBatchingReferencesCreateOK with default headers values
func NewWeaviateBatchingReferencesCreateOK() *WeaviateBatchingReferencesCreateOK {
	return &WeaviateBatchingReferencesCreateOK{}
}

/*WeaviateBatchingReferencesCreateOK handles this case with default header values.

Request Successful. Warning: A successful request does not guarantuee that every batched reference was successfully created. Inspect the response body to see which references succeeded and which failed.
*/
type WeaviateBatchingReferencesCreateOK struct {
	Payload []*models.BatchReferenceResponse
}

func (o *WeaviateBatchingReferencesCreateOK) Error() string {
	return fmt.Sprintf("[POST /batching/references][%d] weaviateBatchingReferencesCreateOK  %+v", 200, o.Payload)
}

func (o *WeaviateBatchingReferencesCreateOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewWeaviateBatchingReferencesCreateUnauthorized creates a WeaviateBatchingReferencesCreateUnauthorized with default headers values
func NewWeaviateBatchingReferencesCreateUnauthorized() *WeaviateBatchingReferencesCreateUnauthorized {
	return &WeaviateBatchingReferencesCreateUnauthorized{}
}

/*WeaviateBatchingReferencesCreateUnauthorized handles this case with default header values.

Unauthorized or invalid credentials.
*/
type WeaviateBatchingReferencesCreateUnauthorized struct {
}

func (o *WeaviateBatchingReferencesCreateUnauthorized) Error() string {
	return fmt.Sprintf("[POST /batching/references][%d] weaviateBatchingReferencesCreateUnauthorized ", 401)
}

func (o *WeaviateBatchingReferencesCreateUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewWeaviateBatchingReferencesCreateForbidden creates a WeaviateBatchingReferencesCreateForbidden with default headers values
func NewWeaviateBatchingReferencesCreateForbidden() *WeaviateBatchingReferencesCreateForbidden {
	return &WeaviateBatchingReferencesCreateForbidden{}
}

/*WeaviateBatchingReferencesCreateForbidden handles this case with default header values.

Forbidden
*/
type WeaviateBatchingReferencesCreateForbidden struct {
	Payload *models.ErrorResponse
}

func (o *WeaviateBatchingReferencesCreateForbidden) Error() string {
	return fmt.Sprintf("[POST /batching/references][%d] weaviateBatchingReferencesCreateForbidden  %+v", 403, o.Payload)
}

func (o *WeaviateBatchingReferencesCreateForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewWeaviateBatchingReferencesCreateUnprocessableEntity creates a WeaviateBatchingReferencesCreateUnprocessableEntity with default headers values
func NewWeaviateBatchingReferencesCreateUnprocessableEntity() *WeaviateBatchingReferencesCreateUnprocessableEntity {
	return &WeaviateBatchingReferencesCreateUnprocessableEntity{}
}

/*WeaviateBatchingReferencesCreateUnprocessableEntity handles this case with default header values.

Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?
*/
type WeaviateBatchingReferencesCreateUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

func (o *WeaviateBatchingReferencesCreateUnprocessableEntity) Error() string {
	return fmt.Sprintf("[POST /batching/references][%d] weaviateBatchingReferencesCreateUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *WeaviateBatchingReferencesCreateUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewWeaviateBatchingReferencesCreateInternalServerError creates a WeaviateBatchingReferencesCreateInternalServerError with default headers values
func NewWeaviateBatchingReferencesCreateInternalServerError() *WeaviateBatchingReferencesCreateInternalServerError {
	return &WeaviateBatchingReferencesCreateInternalServerError{}
}

/*WeaviateBatchingReferencesCreateInternalServerError handles this case with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type WeaviateBatchingReferencesCreateInternalServerError struct {
	Payload *models.ErrorResponse
}

func (o *WeaviateBatchingReferencesCreateInternalServerError) Error() string {
	return fmt.Sprintf("[POST /batching/references][%d] weaviateBatchingReferencesCreateInternalServerError  %+v", 500, o.Payload)
}

func (o *WeaviateBatchingReferencesCreateInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}
