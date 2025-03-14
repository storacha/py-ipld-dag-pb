from typing import TypedDict, Union
import json

from multiformats import CID

from ipld_dag_pb import  encode, decode
from ipld_dag_pb.decode import decode_node
from ipld_dag_pb.encode import encode_node
from ipld_dag_pb.node import PBNode, RawPBNode


# tests mirrored in https://github.com/ipld/js-dag-pb/blob/master/test/test-compat.spec.js

# Hash is raw+identity 0x0001020304 CID(bafkqabiaaebagba)
a_cid = CID.decode(bytearray([1, 85, 0, 5, 0, 1, 2, 3, 4]))


class Case(TypedDict):
    node: Union[RawPBNode, PBNode]
    expected_bytes: str
    expected_form: str


def serialize_node_to_json(node: RawPBNode | PBNode):
    if isinstance(node, RawPBNode):
        serialized_links = []
        serializable_obj = {}

        for link in node.links:
            serialized_links.append(
                {"name": link.name, "t_size": link.t_size, "hash": link.hash.hex()}
            )

        # data should come before links
        if node.data is not None:
            serializable_obj["data"] = node.data.hex()
        serializable_obj["links"] = serialized_links
        return serializable_obj
    elif isinstance(node, PBNode):
        serialized_links = []
        serializable_obj = {}

        for link in node.links:
            link_dict = {}
            if link.name:
                link_dict["name"] = link.name
            if link.t_size:
                link_dict["t_size"] = link.t_size
            link_dict["hash"] = str(link.hash)
            serialized_links.append(link_dict)

        # data should come before links
        if node.data is not None:
            # if zero bytes, serialize to empty string instead
            serializable_obj["data"] = node.data if node.data != bytes(0) else ""
        serializable_obj["links"] = serialized_links
        return serializable_obj


def verify_round_trip(test_case: Case, bypass: bool):
    actual_bytes = (
        encode_node(test_case['node']) if bypass else encode(test_case['node'])  # type: ignore[type-arg]
    )
    assert actual_bytes.hex() == test_case['expected_bytes']
    round_trip_node = decode_node(actual_bytes) if bypass else decode(actual_bytes)
    if round_trip_node.data:
        # this can't be a string, but we're making it so for ease of test
        round_trip_node.data = round_trip_node.data.hex()  # type: ignore[assignment]

    if round_trip_node.links:
        for link in round_trip_node.links:
            if link.hash:
                # this can't be a string, but we're making it so for ease of test
                link.hash = link.hash if bypass else bytes(link.hash)  # type: ignore[assignment]

    actual_form = json.dumps(round_trip_node, default=serialize_node_to_json)
    assert actual_form == test_case["expected_form"]


def test_empty():
    verify_round_trip(
        {
            "node": PBNode(links=[]),
            "expected_bytes": "",
            "expected_form": r'{"links": []}',
        },
        bypass=False
    )


def test_data_zero():
    verify_round_trip(
        {
            "node": PBNode(data=bytes(0)),
            "expected_bytes": '0a00',
            "expected_form": r'{"data": "", "links": []}',
        },
        bypass=False
    )


def test_data_some():
    verify_round_trip(
        {
            "node": PBNode(data=bytearray([0, 1, 2, 3, 4])),
            "expected_bytes": '0a050001020304',
            "expected_form": r'{"data": "0001020304", "links": []}',
        },
        bypass=False
    )


def test_link_zero():
    verify_round_trip(
        {
            "node": PBNode(links=[]),
            "expected_bytes": '',
            "expected_form": r'{"links": []}'
        },
        bypass=False
    )


def test_data_some_links_zero():
        test_case: Case = {
            "node": PBNode(data=bytearray([0, 1, 2, 3, 4]), links=[]),
            "expected_bytes": "0a050001020304",
            "expected_form": r'{"data": "0001020304", "links": []}'
        }
        verify_round_trip(test_case, False)
