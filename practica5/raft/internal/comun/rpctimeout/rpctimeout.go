package rpctimeout

import (
	"fmt"
	"net/rpc"
	"time"
)

func CallTimeout(client *rpc.Client, serviceMethod string, args interface{}, reply interface{}, timeout time.Duration) error {
	done := client.Go(serviceMethod, args, reply, make(chan *rpc.Call, 1)).Done
	
	select {
		case call := <-done:
			return call.Error
		case <-time.After(timeout):
			return fmt.Errorf(
						  "Timeout in CallTimeout with method: %s, args: %v\n",
						  serviceMethod,
						  args)
	}
}



