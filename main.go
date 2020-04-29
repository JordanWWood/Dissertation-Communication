package main

import (
	StateManager "StateManager/proto/StateManager"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"sync"
	"time"
)

const (
	port = ":50051"
)

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	//kp := keepalive.ServerParameters {
	//	Time: 10000,
	//	Timeout: 10000,
	//}
	//grpc.KeepaliveParams(kp)
	s := grpc.NewServer();

	state := &StateService{
		State: new(sync.Map),
		Clients: make(map[string]StateManager.Type),
		StateStreamChannels: make(map[string]chan StateManager.EntityState),
		SubscriptionChannels: make(map[string]chan StateManager.ConnectionRequest),
	}
	StateManager.RegisterStateServer(s, state)
	log.Info("Serving GRPC")

	go syncState(state)
	//go printState(state)
	if err := s.Serve(lis); err != nil {
		log.Error("failed to serve: %v", err)
	}
}

func syncState(service *StateService) {
	for {
		service.StateStreamChannelsMux.Lock()
		for _, v := range service.StateStreamChannels {
			service.State.Range(func (k, v1 interface{}) bool {
				v <- *v1.(StateItem).AsEntityState()
				return true
			})
		}
		service.StateStreamChannelsMux.Unlock()

		time.Sleep(50 * time.Millisecond)
	}
}

func printState(service *StateService) {
	var itr int
	for {
		log.Info("printState")
		service.State.Range(func(k, v interface{}) bool {
			log.Infof("%v Map Item = "+k.(string)+" %v", itr, v.(StateItem))
			return true
		})

		itr++;
		time.Sleep(1 * time.Second)
	}
}
