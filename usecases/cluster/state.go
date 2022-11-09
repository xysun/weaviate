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

package cluster

import (
	"fmt"
	"net"
	"strings"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type State struct {
	list *memberlist.Memberlist
}

type Config struct {
	Hostname       string `json:"hostname" yaml:"hostname"`
	GossipBindPort int    `json:"gossipBindPort" yaml:"gossipBindPort"`
	DataBindPort   int    `json:"dataBindPort" yaml:"dataBindPort"`
	Join           string `json:"join" yaml:"join"`
}

func Init(userConfig Config, logger logrus.FieldLogger) (*State, error) {
	cfg := memberlist.DefaultLocalConfig()
	cfg.LogOutput = newLogParser(logger)

	if userConfig.Hostname != "" {
		cfg.Name = userConfig.Hostname
	}

	if userConfig.GossipBindPort != 0 {
		cfg.BindPort = userConfig.GossipBindPort
	}

	var err error
	cfg.AdvertiseAddr, err = parseAdvertiseAddr(userConfig)
	if err != nil {
		return nil, fmt.Errorf("parse advertise addr: %w", err)
	}

	list, err := memberlist.Create(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "create member list")
	}

	_, err = net.LookupIP(cfg.AdvertiseAddr)
	if err != nil {
		logger.WithField("action", "cluster_attempt_join").
			WithField("remote_hostname", cfg.AdvertiseAddr).
			WithError(err).
			Warn("specified hostname to join cluster cannot be resolved. This is fine" +
				"if this is the first node of a new cluster, but problematic otherwise.")
	} else {
		_, err := list.Join([]string{cfg.AdvertiseAddr})
		if err != nil {
			return nil, errors.Wrap(err, "join cluster")
		}
	}

	return &State{list: list}, nil
}

func parseAdvertiseAddr(userConfig Config) (string, error) {
	if userConfig.Join != "" {
		joinAddr := strings.Split(userConfig.Join, ",")
		joinAddr = strings.Split(joinAddr[0], ":")
		addrs, err := net.LookupHost(joinAddr[0])
		if err != nil {
			return "", fmt.Errorf("lookup host: %w", err)
		}
		return addrs[0], nil
	}
	return "127.0.0.1", nil
}

// Hostnames for all live members, except self. Use AllHostnames to include
// self, prefixes the data port.
func (s *State) Hostnames() []string {
	mem := s.list.Members()
	out := make([]string, len(mem))

	i := 0
	for _, m := range mem {
		if m.Name == s.list.LocalNode().Name {
			continue
		}
		// TODO: how can we find out the actual data port as opposed to relying on
		// the convention that it's 1 higher than the gossip port
		out[i] = fmt.Sprintf("%s:%d", m.Addr.String(), m.Port+1)
		i++
	}

	return out[:i]
}

// AllHostnames for live members, including self.
func (s *State) AllHostnames() []string {
	mem := s.list.Members()
	out := make([]string, len(mem))

	for i, m := range mem {
		// TODO: how can we find out the actual data port as opposed to relying on
		// the convention that it's 1 higher than the gossip port
		out[i] = fmt.Sprintf("%s:%d", m.Addr.String(), m.Port+1)
	}

	return out
}

// All node names (not their hostnames!) for live members, including self.
func (s *State) AllNames() []string {
	mem := s.list.Members()
	out := make([]string, len(mem))

	for i, m := range mem {
		out[i] = m.Name
	}

	return out
}

// All node names (not their hostnames!) for live members, including self.
func (s *State) NodeCount() int {
	return s.list.NumMembers()
}

func (s *State) LocalName() string {
	return s.list.LocalNode().Name
}

func (s *State) ClusterHealthScore() int {
	return s.list.GetHealthScore()
}

func (s *State) NodeHostname(nodeName string) (string, bool) {
	for _, mem := range s.list.Members() {
		if mem.Name == nodeName {
			// TODO: how can we find out the actual data port as opposed to relying on
			// the convention that it's 1 higher than the gossip port
			return fmt.Sprintf("%s:%d", mem.Addr.String(), mem.Port+1), true
		}
	}

	return "", false
}
