package main

import (
	"github.com/gorilla/websocket"
	"net/url"
	"arsyncfs/agent"
	"arsyncfs"
	"arsyncfs/fs"
	"fmt"
)

func main() {

	u := url.URL{Scheme: "ws", Host: "localhost:8000", Path: "/"}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)

	if err != nil {

	}

	req := agent.Request{
		Op: 0,
		RemoteNode: fs.RemoteNode{
			IsRoot: false,
			RemotePath: arsyncfs.RemotePath{
				Hostname: "localhost",
				Port:     8000,
				Path:     "/Users/sourabh/Downloads/tenor.gif",
			},
		},
	}

	//js, _ := json.Marshal(req)
	fmt.Println(req)
	err = c.WriteJSON(req)

	fmt.Println(err)

	resp := agent.Response{}
	err = c.ReadJSON(&resp)

	//fmt.Println(err.Error())

	fmt.Println(resp.Op)
	fmt.Println(resp.RemoteNode)
	fmt.Println(resp.Response.(agent.Stat))
	//c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))

	c.Close()
}
