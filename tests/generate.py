from pathlib import Path
import time
from io import BytesIO
import json

from mcap.writer import Writer
from mcap_ros2.writer import Writer as Ros2Writer
from std_msgs.msg import String  # type: ignore
from .string_message_pb2 import StringMessage


def generate_ros1_data():
    output = BytesIO()
    writer = Writer(output)
    writer.start(profile="ros1", library="test")
    string_schema_id = writer.register_schema(
        name=String._type, encoding="ros1msg", data=String._full_text.encode()  # type: ignore
    )
    string_channel_id = writer.register_channel(
        topic="chatter", message_encoding="ros1", schema_id=string_schema_id
    )

    for i in range(1, 11):
        s = String(data=f"string message {i}")
        buff = BytesIO()
        s.serialize(buff)  # type: ignore
        writer.add_message(
            channel_id=string_channel_id,
            log_time=time.time_ns(),
            data=buff.getvalue(),
            publish_time=time.time_ns(),
        )
    writer.finish()
    return output.getvalue()


def generate_ros2_data():
    output = BytesIO()
    with Ros2Writer(output) as writer:
        schema = writer.register_msgdef(String._type, String._full_text)

        for i in range(1, 11):
            writer.write_message("/chatter", schema, {"data": f"string message {i}"})
    return output.getvalue()


def generate_json_data():
    output = BytesIO()
    mcap_writer = Writer(output)
    mcap_writer.start(profile="", library="test")
    schema_id = mcap_writer.register_schema(
        name="mood",
        encoding="jsonschema",
        data=json.dumps(
            {
                "type": "object",
                "properties": {
                    "happy": {"type": "boolean"},
                    "level": {"type": "number"},
                },
            }
        ).encode("utf-8"),
    )
    channel_id = mcap_writer.register_channel(
        topic="moods",
        message_encoding="json",
        schema_id=schema_id,
    )
    for i in range(1, 11):
        m = json.dumps({"happy": True, "level": i}).encode("utf-8")
        mcap_writer.add_message(
            channel_id=channel_id,
            log_time=time.time_ns(),
            data=m,
            publish_time=time.time_ns(),
        )
    mcap_writer.finish()
    return output.getvalue()


def generate_protobuf_data():
    output = BytesIO()
    mcap_writer = Writer(output)
    mcap_writer.start(profile="protobuf", library="test")

    schema_id = mcap_writer.register_schema(
        name="StringMessage",
        encoding="protobuf",
        data=(Path(__file__).parent / "string_message.fds").read_bytes(),
    )
    channel_id = mcap_writer.register_channel(
        topic="chatter", message_encoding="protobuf", schema_id=schema_id
    )

    for i in range(1, 11):
        s = StringMessage(data=f"string message {i}")
        mcap_writer.add_message(
            channel_id=channel_id,
            log_time=time.time_ns(),
            data=s.SerializeToString(),  # type: ignore
            publish_time=time.time_ns(),
        )
    mcap_writer.finish()
    return output.getvalue()
