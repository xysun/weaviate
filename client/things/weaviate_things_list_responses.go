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
	"fmt"
	"io"

	"github.com/go-openapi/runtime"

	strfmt "github.com/go-openapi/strfmt"

	models "github.com/semi-technologies/weaviate/entities/models"
)

// WeaviateThingsListReader is a Reader for the WeaviateThingsList structure.
type WeaviateThingsListReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *WeaviateThingsListReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {

	case 200:
		result := NewWeaviateThingsListOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil

	case 401:
		result := NewWeaviateThingsListUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	case 403:
		result := NewWeaviateThingsListForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	case 404:
		result := NewWeaviateThingsListNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	case 500:
		result := NewWeaviateThingsListInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	default:
		return nil, runtime.NewAPIError("unknown error", response, response.Code())
	}
}

// NewWeaviateThingsListOK creates a WeaviateThingsListOK with default headers values
func NewWeaviateThingsListOK() *WeaviateThingsListOK {
	return &WeaviateThingsListOK{}
}

/*WeaviateThingsListOK handles this case with default header values.

Successful response.
*/
type WeaviateThingsListOK struct {
	Payload *models.ThingsListResponse
}

func (o *WeaviateThingsListOK) Error() string {
	return fmt.Sprintf("[GET /things][%d] weaviateThingsListOK  %+v", 200, o.Payload)
}

func (o *WeaviateThingsListOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ThingsListResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewWeaviateThingsListUnauthorized creates a WeaviateThingsListUnauthorized with default headers values
func NewWeaviateThingsListUnauthorized() *WeaviateThingsListUnauthorized {
	return &WeaviateThingsListUnauthorized{}
}

/*WeaviateThingsListUnauthorized handles this case with default header values.

Unauthorized or invalid credentials.
*/
type WeaviateThingsListUnauthorized struct {
}

func (o *WeaviateThingsListUnauthorized) Error() string {
	return fmt.Sprintf("[GET /things][%d] weaviateThingsListUnauthorized ", 401)
}

func (o *WeaviateThingsListUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewWeaviateThingsListForbidden creates a WeaviateThingsListForbidden with default headers values
func NewWeaviateThingsListForbidden() *WeaviateThingsListForbidden {
	return &WeaviateThingsListForbidden{}
}

/*WeaviateThingsListForbidden handles this case with default header values.

Forbidden
*/
type WeaviateThingsListForbidden struct {
	Payload *models.ErrorResponse
}

func (o *WeaviateThingsListForbidden) Error() string {
	return fmt.Sprintf("[GET /things][%d] weaviateThingsListForbidden  %+v", 403, o.Payload)
}

func (o *WeaviateThingsListForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewWeaviateThingsListNotFound creates a WeaviateThingsListNotFound with default headers values
func NewWeaviateThingsListNotFound() *WeaviateThingsListNotFound {
	return &WeaviateThingsListNotFound{}
}

/*WeaviateThingsListNotFound handles this case with default header values.

Successful query result but no resource was found.
*/
type WeaviateThingsListNotFound struct {
}

func (o *WeaviateThingsListNotFound) Error() string {
	return fmt.Sprintf("[GET /things][%d] weaviateThingsListNotFound ", 404)
}

func (o *WeaviateThingsListNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewWeaviateThingsListInternalServerError creates a WeaviateThingsListInternalServerError with default headers values
func NewWeaviateThingsListInternalServerError() *WeaviateThingsListInternalServerError {
	return &WeaviateThingsListInternalServerError{}
}

/*WeaviateThingsListInternalServerError handles this case with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type WeaviateThingsListInternalServerError struct {
	Payload *models.ErrorResponse
}

func (o *WeaviateThingsListInternalServerError) Error() string {
	return fmt.Sprintf("[GET /things][%d] weaviateThingsListInternalServerError  %+v", 500, o.Payload)
}

func (o *WeaviateThingsListInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}
