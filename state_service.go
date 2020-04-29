package main

import (
	StateManager "StateManager/Dissertation-Protocol/v1/StateManager"
	"context"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
	"time"
)

type StateItem struct {
	Id            string
	prefabName    string
	membersList   []*StateManager.Member
	membersByName map[string]*StateManager.Member

	UpdateChannels []chan *StateManager.Member
}

type StateService struct {
	State *sync.Map

	Clients map[string]StateManager.Type

	StateStreamChannels    map[string]chan StateManager.EntityState
	StateStreamChannelsMux sync.Mutex

	SubscriptionChannels    map[string]chan StateManager.ConnectionRequest
	SubscriptionChannelMux  map[string]sync.Mutex
	SubscriptionChannelsMux sync.Mutex
}

func (item StateItem) AsEntityState() *StateManager.EntityState {
	return &StateManager.EntityState{
		Id:         item.Id,
		PrefabName: item.prefabName,
		Members:    item.membersList,
	}
}

func (s StateService) FullLoad(context.Context, *StateManager.ConnectionRequest) (*StateManager.PackedState, error) {
	var packet StateManager.PackedState

	s.State.Range(func(k, v interface{}) bool {
		packet.State = append(packet.State, &StateManager.EntityState{
			Id:         k.(string),
			PrefabName: v.(StateItem).prefabName,
			Members:    v.(StateItem).membersList,
		})

		return true
	})
	log.Info(packet)

	return &packet, nil
}

func (s StateService) Connect(context context.Context, req *StateManager.InitialConnectionRequest) (*StateManager.ConnectionRequest, error) {
	log.Info("Observing connection!")
	id := uuid.New()
	log.Info("Assigned id: " + id.String())
	s.Clients[id.String()] = req.Type

	res := StateManager.ConnectionRequest{
		Token: id.String(),
		Type:  req.Type,
	}

	for _, v := range s.SubscriptionChannels {
		log.Info("Announcing presence: " + id.String())
		v <- res
	}

	return &res, nil
}

func (s StateService) UpdateObject(ctx context.Context, req *StateManager.EntityEditRequest) (*StateManager.GenericResponse, error) {
	if _, ok := s.Clients[req.Token]; !ok {
		log.Warn("Unknown client attempted to UpdateObject: " + req.Token)
		return nil, status.Error(codes.PermissionDenied, "No VALID token was found")
	}

	if _, ok := s.State.Load(req.EntityId); !ok {
		return &StateManager.GenericResponse{
			Successful: false,
			Error:      "Unknown Entity",
		}, nil
	}

	for _, item := range req.State.Members {
		entity, _ := s.State.Load(req.EntityId)
		if _, ok := entity.(StateItem).membersByName[item.MemberName]; !ok {
			return &StateManager.GenericResponse{
				Successful: false,
				Error:      "Unknown Member " + item.MemberName,
			}, nil
		}

		if entity.(StateItem).membersByName[item.MemberName].AuthoritativeMember != req.Token {
			return &StateManager.GenericResponse{
				Successful: false,
				Error:      "Unauthorised! " + req.EntityId + " " + item.MemberName,
			}, nil
		}

		entity.(StateItem).membersByName[item.MemberName].Data = item.Data
	}

	return &StateManager.GenericResponse{
		Successful: true,
		Error:      "",
	}, nil
}

func (s StateService) CreateObject(ctx context.Context, req *StateManager.EntityEditRequest) (*StateManager.GenericResponse, error) {
	if _, ok := s.Clients[req.Token]; !ok {
		log.Warn("Unknown client attempted to CreateObject: " + req.Token)
		return nil, status.Error(codes.PermissionDenied, "No VALID token was found")
	}

	log.Info(req.EntityId)
	if _, ok := s.State.Load(req.EntityId); ok {
		log.Info("Already exists")
		return &StateManager.GenericResponse{
			Successful: false,
			Error:      "Object already exists " + req.EntityId,
		}, nil
	}

	members := make(map[string]*StateManager.Member)
	for _, v := range req.State.Members {
		members[v.MemberName] = v
	}

	log.Info("Adding " + req.EntityId)
	s.State.Store(req.EntityId, StateItem{
		Id:            req.State.Id,
		prefabName:    req.State.PrefabName,
		membersList:   req.State.Members,
		membersByName: members,
	})
	log.Info("Added " + req.EntityId)

	return &StateManager.GenericResponse{
		Successful: true,
		Error:      "",
	}, nil
}

func (s StateService) RemoveObject(context.Context, *StateManager.EntityRequest) (*StateManager.GenericResponse, error) {
	panic("implement me")
}

func (s StateService) RetrieveAuthority(context.Context, *StateManager.EntityRequest) (*StateManager.AuthorityResponse, error) {
	panic("implement me")
}

func (s StateService) RetrieveState(context.Context, *StateManager.EntityRequest) (*StateManager.EntityState, error) {
	panic("implement me")
}

func (s StateService) StateStream(req *StateManager.ConnectionRequest, stream StateManager.State_StateStreamServer) error {
	log.Info("New client connecting to StateStream " + req.Token)
	if _, ok := s.Clients[req.Token]; !ok {
		log.Warn("Unknown client attempted to StateStream: " + req.Token)
		return status.Error(codes.PermissionDenied, "No VALID token was found")
	}

	ch := make(chan StateManager.EntityState)
	s.StateStreamChannels[req.Token] = ch

	for {
		for data := range ch {
			if err := stream.Send(&data); err != nil {
				log.Error("StateStream closed unexpectedly: " + err.Error())
				return err
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (s StateService) SubscriptionStream(req *StateManager.ConnectionRequest, stream StateManager.State_SubscriptionStreamServer) error {
	log.Info("New client connecting to SubscriptionStream " + req.Token)
	if _, ok := s.Clients[req.Token]; !ok {
		log.Warn("Unknown client attempted to SubscriptionStream: " + req.Token)
		return status.Error(codes.PermissionDenied, "No VALID token was found")
	}

	ch := make(chan StateManager.ConnectionRequest)
	s.SubscriptionChannels[req.Token] = ch

	for {
		data := <-ch
		log.Info("Receiving connection notification")
		if err := stream.Send(&data); err != nil {
			log.Error("SubscriptionStream closed unexpectedly: " + err.Error())

			close(ch)
			ch = nil
			delete(s.SubscriptionChannels, req.Token)
			return err
		}
	}

	return nil
}
