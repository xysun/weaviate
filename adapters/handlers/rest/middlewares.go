//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/rs/cors"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/state"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/swagger_middleware"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/modules"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
)

// The middleware configuration is for the handler executors. These do not apply to the swagger.json document.
// The middleware executes after routing but before authentication, binding and validation
//
// we are setting the middlewares from within configureAPI, as we need access
// to some resources which are not exposed
func makeSetupMiddlewares(appState *state.State) func(http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.String() == "/v1/.well-known/openid-configuration" {
				handler.ServeHTTP(w, r)
				return
			}
			appState.AnonymousAccess.Middleware(handler).ServeHTTP(w, r)
		})
	}
}

func addHandleRoot(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() == "/" {
			w.Header().Add("Location", "/v1")
			w.WriteHeader(http.StatusMovedPermanently)
			w.Write([]byte(`{"links":{"href":"/v1","name":"api v1","documentationHref":` +
				`"https://weaviate.io/developers/weaviate/current/"}}`))
			return
		}

		next.ServeHTTP(w, r)
	})
}

func makeAddModuleHandlers(modules *modules.Provider) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		mux := http.NewServeMux()

		for _, mod := range modules.GetAll() {
			prefix := fmt.Sprintf("/v1/modules/%s", mod.Name())
			mux.Handle(fmt.Sprintf("%s/", prefix),
				http.StripPrefix(prefix, mod.RootHandler()))
		}

		prefix := "/v1/modules"
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if url := r.URL.String(); len(url) > len(prefix) && url[:len(prefix)] == prefix {
				mux.ServeHTTP(w, r)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func makeRESTVectorSearch(appState *state.State) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		prefix := "/v1/objects/SemanticUnit/_search"
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if url := r.URL.String(); len(url) >= len(prefix) && url[:len(prefix)] == prefix {
				serveRESTVectorSearch(appState, w, r)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

type RESTSearchParams struct {
	NearVector RESTSearchParamsNearVector `json:"nearVector"`
	Limit      int                        `json:"limit"`
}

type RESTSearchParamsNearVector struct {
	Vector []float32 `json:"vector"`
}

func serveRESTVectorSearch(appState *state.State, w http.ResponseWriter, r *http.Request) {
	className := "SemanticUnit"
	var params RESTSearchParams

	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		fmt.Printf("returning bad request\n")
		return
	}

	results := search.PreAllocateRawResults(params.Limit, 1_500)
	err := appState.DB.PreallocatedVectorSearch(r.Context(), traverser.GetParams{
		ClassName: className,
		Pagination: &filters.Pagination{
			Limit: params.Limit,
		},
		SearchVector: params.NearVector.Vector,
	}, results)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		fmt.Printf("returning search error\n")
		return
	}

	if err := results.WriteJSON(w); err != nil {
		fmt.Println(err.Error())
		w.Write([]byte(fmt.Sprintf(`{"error":"%s"}`, err.Error())))
	}
}

// The middleware configuration happens before anything, this middleware also applies to serving the swagger.json document.
// So this is a good place to plug in a panic handling middleware, logging and metrics
// Contains "x-api-key", "x-api-token" for legacy reasons, older interfaces might need these headers.
func makeSetupGlobalMiddleware(appState *state.State) func(http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		handleCORS := cors.New(cors.Options{
			OptionsPassthrough: true,
			AllowedMethods:     []string{"POST", "PUT", "DELETE", "GET", "PATCH"},
		}).Handler
		handler = handleCORS(handler)
		handler = swagger_middleware.AddMiddleware([]byte(SwaggerJSON), handler)
		handler = makeAddLogging(appState.Logger)(handler)
		handler = addPreflight(handler)
		handler = addLiveAndReadyness(handler)
		handler = addHandleRoot(handler)
		handler = makeAddModuleHandlers(appState.Modules)(handler)
		handler = makeRESTVectorSearch(appState)(handler)
		handler = addInjectHeadersIntoContext(handler)

		return handler
	}
}

func makeAddLogging(logger logrus.FieldLogger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger.
				WithField("action", "restapi_request").
				WithField("method", r.Method).
				WithField("url", r.URL).
				Debug("received HTTP request")
			next.ServeHTTP(w, r)
		})
	}
}

func addPreflight(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "*")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, Batch")
			return
		}

		next.ServeHTTP(w, r)
	})
}

func addInjectHeadersIntoContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		changed := false
		for k, v := range r.Header {
			if strings.HasPrefix(k, "X-") {
				ctx = context.WithValue(ctx, k, v)
				changed = true
			}
		}

		if changed {
			next.ServeHTTP(w, r.Clone(ctx))
		} else {
			next.ServeHTTP(w, r)
		}
	})
}

func addLiveAndReadyness(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() == "/v1/.well-known/live" {
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.URL.String() == "/v1/.well-known/ready" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
