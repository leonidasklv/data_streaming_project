#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START cloudrun_pubsub_server_setup]
# [START run_pubsub_server_setup]
import base64
import os

from concurrent import futures
from google.cloud import pubsub_v1
from flask import Flask, request


def get_callback(
    publish_future: pubsub_v1.publisher.futures.Future, data: str
) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
    def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
        try:
            # Wait 60 seconds for the publish call to succeed.
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Publishing {data} timed out.")

    return callback


app = Flask(__name__)
# [END run_pubsub_server_setup]
# [END cloudrun_pubsub_server_setup]


# [START cloudrun_pubsub_handler]
# [START run_pubsub_handler]
@app.route("/", methods=["POST"])
def index():
    envelope = request.get_json()
    if not envelope:
        msg = "no message received"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    if not isinstance(envelope, dict) or "message" not in envelope:
        msg = "invalid message format"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    pubsub_message = envelope["message"]
    #print("Message Noumber: " , envelope)

    name = "World"

    if isinstance(pubsub_message, dict): ##and 'data' in pubsub_message:
        name = base64.b64decode(pubsub_message['data']).decode("utf-8").strip()

    print("Message Noumber: " , name)


"""Pushing the message to a Pub/Sub topic with an error handler."""
# In the above lines we are receiving a json file and now we want to push it to a pub/sub topic.
# Bellow we push the received json file ( and more precise the data of the received message) to an already created topic. 
# We can create multiple topics and have the destination that we want to sent them on the json file.
# That maybe helpfull to seperate the receiving messages. Moreover later it will be efficient on the dataflow to choose one topic of received messages to 
# store them in the right querry or to stream eventualy some of them and not all of them. 
# 

# This change of each diferent topic or project
    project_id = "majestic-device-333311"
    topic_id = "data-receiver"
    
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    publish_futures = []

    # When you publish a message, the client returns a future.
    publish_future = publisher.publish(topic_path, data.encode("utf-8"))
    # Non-blocking. Publish failures are handled in the callback function.
    publish_future.add_done_callback(get_callback(publish_future, data))
    publish_futures.append(publish_future)

    # Wait for all the publish futures to resolve before exiting.
    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

    print(f"Published messages with error handler to {topic_path}.")

    return ("", 204)


# [END run_pubsub_handler]
# [END cloudrun_pubsub_handler]


if __name__ == "__main__":
    PORT = int(os.getenv("PORT")) if os.getenv("PORT") else 8080

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    app.run(host="127.0.0.1", port=PORT, debug=True)
