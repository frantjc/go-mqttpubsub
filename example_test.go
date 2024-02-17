// Copyright 2019 The Go Cloud Development Kit Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mqttpubsub_test

import (
	"context"
	"log"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	mqttpubsub "github.com/frantjc/go-pubsubmqtt"
)

var ctx = context.Background()

func ExampleOpenTopic() {
	opts := mqtt.NewClientOptions()
	opts = opts.AddBroker("mqtt://mqtt.example.com")
	opts.ClientID = "exampleClient"
	cli := mqtt.NewClient(opts)
	defer cli.Disconnect(0)
	token := cli.Connect()
	if token.Wait() && token.Error() != nil {
		log.Println(token.Error())
		return
	}

	topic, err := mqttpubsub.OpenTopic(mqttpubsub.NewPublisher(cli, 0, 0), "example.mysubject", nil)
	if err != nil {
		log.Println(err)
		return
	}
	defer func() {
		_ = topic.Shutdown(ctx)
	}()
}

func ExampleOpenSubscription() {
	opts := mqtt.NewClientOptions()
	opts = opts.AddBroker("mqtt://mqtt.example.com")
	opts.ClientID = "exampleClient"
	cli := mqtt.NewClient(opts)
	defer cli.Disconnect(0)
	token := cli.Connect()
	if token.Wait() {
		if err := token.Error(); err != nil {
			log.Println(token.Error())
			return
		}
	}

	subscription, err := mqttpubsub.OpenSubscription(
		mqttpubsub.NewSubscriber(cli, 0, 0),
		"example.mysubject",
		nil)
	if err != nil {
		log.Println(token.Error())
		return
	}
	defer func() {
		_ = subscription.Shutdown(ctx)
	}()
}
