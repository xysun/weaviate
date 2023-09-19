package schemav2

import (
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type RequestAddClass struct {
	Class models.Class
	State sharding.State
}

type RequestUpdateClass struct {
	Class *models.Class
	State *sharding.State
}

type RequestAddProperty struct {
	models.Property
}
