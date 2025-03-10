import pytest
from multiformats import CID
from ipld_dag_pb import decode, encode, prepare, PBNode


def test_prepare_encode_an_empty_node() -> None:
    prepared = prepare({})
    assert prepared == PBNode()

    result = encode(prepared)
    assert result == bytes()

    node = decode(result)
    assert node.data is None


def test_prepare_encode_node_with_data() -> None:
    data = bytes([0, 1, 2, 3, 4])
    prepared = prepare(data)
    assert prepared == PBNode(data)

    result = encode(prepared)
    assert isinstance(result, memoryview)

    node = decode(result)
    assert node.data == data


def test_prepare_encode_node_with_string_data() -> None:
    data = "some bytes"
    prepared = prepare({"data": data})
    assert prepared == PBNode(data.encode("utf-8"))

    result = encode(prepared)
    assert isinstance(result, memoryview)

    node = decode(result)
    assert bytes(node.data).decode("utf-8") == data


def test_prepare_encode_node_with_bare_string() -> None:
    data = "some bytes"
    prepared = prepare(data)
    assert prepared == PBNode(data.encode("utf-8"))

    result = encode(prepared)
    assert isinstance(result, memoryview)

    node = decode(result)
    assert bytes(node.data).decode("utf-8") == data


def test_prepare_encode_node_with_links() -> None:
    links = [CID.decode("QmWDtUQj38YLW8v3q4A6LwPn4vYKEbuKWpgSm6bjKW6Xfe")]
    prepared = prepare({"links": links})
    assert prepared.links[0].hash == links[0]

    result = encode(prepared)
    assert isinstance(result, memoryview)

    node = decode(result)
    assert len(links) == len(node.links)

    for i in range(len(links)):
        assert links[i] == node.links[i].hash
        assert node.links[i].name is None
        assert node.links[i].t_size is None


def test_ignore_invalid_properties_when_preparing() -> None:
    prepared = prepare({"foo": "bar"})
    assert prepared == PBNode()

    result = encode(prepared)
    assert result == bytes()


def test_prepare_encode_node_with_links_and_sorting() -> None:
    orig_links = [
        {
            "name": "some other link",
            "hash": CID.decode("QmXg9Pp2ytZ14xgmQjYEiHjVjMFXzCVVEcRTWJBmLgR39V"),
            "t_size": 8,
        },
        {
            "name": "some link",
            "hash": CID.decode("QmXg9Pp2ytZ14xgmQjYEiHjVjMFXzCVVEcRTWJBmLgR39U"),
            "t_size": 100000000,
        },
    ]
    some_data = "some data".encode("utf-8")
    node = {"data": some_data, "links": orig_links}

    prepared = prepare(node)
    assert list(map(lambda l: l.name, prepared.links)) == [
        "some link",
        "some other link",
    ]
    byts = encode(prepared)
    expected_bytes = "12340a2212208ab7a6c5e74737878ac73863cb76739d15d4666de44e5756bf55a2f9e9ab5f431209736f6d65206c696e6b1880c2d72f12370a2212208ab7a6c5e74737878ac73863cb76739d15d4666de44e5756bf55a2f9e9ab5f44120f736f6d65206f74686572206c696e6b18080a09736f6d652064617461"
    assert expected_bytes == bytes(byts).hex()

    reconstituted = decode(byts)

    # check sorting
    assert list(map(lambda l: l.name, reconstituted.links)) == [
        "some link",
        "some other link",
    ]
