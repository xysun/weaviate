package modneuralmagic

import (
	"context"
	"net/http"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/state"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/modules/text2vec-neuralmagic/client"
	"github.com/semi-technologies/weaviate/modules/text2vec-neuralmagic/neartext"
	"github.com/sirupsen/logrus"
)

const (
	Name = "text2vec-neuralmagic"
)

func New() *NeuralMagicModule {
	return &NeuralMagicModule{}
}

type NeuralMagicModule struct {
	logger          logrus.FieldLogger
	storageProvider moduletools.StorageProvider
	//extensions                   *extensions.RESTHandlers
	//concepts                     *concepts.RESTHandlers
	//vectorizer                   *localvectorizer.Vectorizer
	//configValidator              configValidator
	graphqlProvider modulecapabilities.GraphQLArguments
	//additionalPropertiesProvider modulecapabilities.AdditionalProperties
	//searcher                     modulecapabilities.Searcher
	remote *client.Client
	//classifierContextual         modulecapabilities.Classifier
	//logger                       logrus.FieldLogger
	//nearTextTransformer          modulecapabilities.TextTransform
}

func (m *NeuralMagicModule) Name() string {
	return "text2vec-contextionary"
}

func (m *NeuralMagicModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2Vec
}

func (m *NeuralMagicModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	m.storageProvider = params.GetStorageProvider()
	appState, ok := params.GetAppState().(*state.State)
	if !ok {
		return errors.Errorf("appState is not a *state.State")
	}

	m.logger = appState.Logger

	remote, err := client.NewClient(m.logger)
	if err != nil {
		return errors.Wrap(err, "init remote client")
	}
	m.remote = remote
	return nil
}

func (m *NeuralMagicModule) RootHandler() http.Handler {
	return nil
}

// InitExtension where you add nearText capabilities
func (m *NeuralMagicModule) InitExtension(_ []modulecapabilities.Module) error {
	if err := m.initGraphqlProvider(); err != nil {
		return errors.Wrap(err, "init graphql provider")
	}

	return nil
}

func (m *NeuralMagicModule) initGraphqlProvider() error {
	m.graphqlProvider = neartext.New()
	return nil
}

var (
	_ = modulecapabilities.Module(New())
)
