import pytest
from multiformats import CID
from ipld_dag_pb import decode, encode, prepare, PBNode
from ipld_dag_pb.util import as_link, create_link, create_node


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


def create_node_with_stable_sorted_links():
    links = [
        {
          "name": '',
          "hash": CID.decode('QmUGhP2X8xo9dsj45vqx1H6i5WqPqLqmLQsHTTxd3ke8mp'),
          "t_size": 262158
        }, 
        {
          "name": '',
          "hash": CID.decode('QmP7SrR76KHK9A916RbHG1ufy2TzNABZgiE23PjZDMzZXy'),
          "t_size": 262158
        },
        {
          "name": '',
          "hash": CID.decode('QmQg1v4o9xdT3Q14wh4S7dxZkDjyZ9ssFzFzyep1YrVJBY'),
          "t_size": 262158
        },
        {
          "name": '',
          "hash": CID.decode('QmdP6fartWRrydZCUjHgrJ4XpxSE4SAoRsWJZ1zJ4MWiuf'),
          "t_size": 262158
        },
        {
          "name": '',
          "hash": CID.decode('QmNNjUStxtMC1WaSZYiDW6CmAUrvd5Q2e17qnxPgVdwrwW'),
          "t_size": 262158
        },
        {
          "name": '',
          "hash": CID.decode('QmWJwqZBJWerHsN1b7g4pRDYmzGNnaMYuD3KSbnpaxsB2h'),
          "t_size": 262158
        },
        {
          "name": '',
          "hash": CID.decode('QmRXPSdysBS3dbUXe6w8oXevZWHdPQWaR2d3fggNsjvieL'),
          "t_size": 262158
        },
        {
          "name": '',
          "hash": CID.decode('QmTUZAXfws6zrhEksnMqLxsbhXZBQs4FNiarjXSYQqVrjC'),
          "t_size": 262158
        },
        {
          "name": '',
          "hash": CID.decode('QmNNk7dTdh8UofwgqLNauq6N78DPc6LKK2yBs1MFdx7Mbg'),
          "t_size": 262158
        },
        {
          "name": '',
          "hash": CID.decode('QmW5mrJfyqh7B4ywSvraZgnWjS3q9CLiYURiJpCX3aro5i'),
          "t_size": 262158
        },
        {
          "name": '',
          "hash": CID.decode('QmTFHZL5CkgNz19MdPnSuyLAi6AVq9fFp81zmPpaL2amED'),
          "t_size": 262158
        }
    ]

    some_data = "some data".encode("utf-8")
    node = PBNode(
        data=some_data, links=[as_link(link) for link in links]
    )

    prepared = prepare({"data": some_data, "links": links})
    assert prepared == node
    reconstituted = decode(encode(node))

    # sorting
    assert list(map(lambda l: l.hash, reconstituted.links)) == list(
        map(lambda l: l["hash"], links)
    )


def test_prepare_create_link_with_empty_link_name():
    node = {
        "data": "hello".encode(),
        "links": [
            CID.decode("QmXg9Pp2ytZ14xgmQjYEiHjVjMFXzCVVEcRTWJBmLgR39U")
        ]
    }
    expected = PBNode(
        data=node["data"],
        links=[as_link({"hash": node["links"]})]
    )
    prepared = prepare(node)
    assert prepared == expected


def test_prepare_create_link_with_undefined_name():
    node = {
        "data": "hello".encode(),
        "links": [
            {
                "t_size": 10,
                "hash": CID.decode("QmXg9Pp2ytZ14xgmQjYEiHjVjMFXzCVVEcRTWJBmLgR39U")
            }
        ]
    }
    expected = PBNode(
        data=node["data"],
        links=[as_link({"hash": node["links"]})]
    )
    prepared = prepare(node)
    assert prepared == expected

    reconstituted = decode(encode(prepared))
    assert reconstituted == expected


def test_create_node_with_data_and_links():
    """Test creating a node with data and links"""
    data = bytes([0, 1, 2, 3, 4])
    cid1 = CID.decode("QmWDtUQj38YLW8v3q4A6LwPn4vYKEbuKWpgSm6bjKW6Xfe")
    cid2 = CID.decode("bafyreifepiu23okq5zuyvyhsoiazv2icw2van3s7ko6d3ixl5jx2yj2yhu")

    links = [
        create_link(cid1, "link1", 100),
        create_link(cid2, "link2", 200)
    ]

    node = create_node(data, links)
    result = encode(node)
    decoded = decode(result)

    assert decoded.data == data
    assert len(decoded.links) == 2
    assert decoded.links[0].hash == cid1
    assert decoded.links[0].name == "link1"
    assert decoded.links[0].t_size == 100
    assert decoded.links[1].hash == cid2
    assert decoded.links[1].name == "link2"
    assert decoded.links[1].t_size == 200


def test_create_link():
    """Test creating a link"""
    cid = CID.decode("QmWDtUQj38YLW8v3q4A6LwPn4vYKEbuKWpgSm6bjKW6Xfe")
    link = create_link(cid, "test-link", 1234)

    assert link.hash == cid
    assert link.name == "test-link"
    assert link.t_size == 1234


def test_bad_forms():
    """Test validation failure with bad forms"""
    # Missing hash
    with pytest.raises(Exception):
        encode(prepare({"links": [{"name": "missing-hash"}]}))

    # Invalid data type
    with pytest.raises(Exception):
        encode(prepare({"data": 123}))  # not bytes-like

    # Links not a list
    with pytest.raises(Exception):
        encode(prepare({"links": "not a list"}))

    # Link is not a dict
    with pytest.raises(Exception):
        encode(prepare({"links": ["not a dict"]}))
