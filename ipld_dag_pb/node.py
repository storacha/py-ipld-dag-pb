from typing import Optional
from multiformats import CID

"""
PBNode and PBLink match the DAG-PB logical format, as described at:
https://github.com/ipld/specs/blob/master/block-layer/codecs/dag-pb.md#logical-format
"""


class PBLink:
    name: Optional[str]
    t_size: Optional[int]
    hash: CID


class PBNode:
    data: Optional[bytes]
    links: list[PBLink]


"""
Raw versions of PBNode and PBLink used internally to deal with the underlying
encode/decode byte interface.
A future iteration could make encode.py and decode.py aware of PBNode and PBLink
specifics (including CID and optionals).
"""


class RawPBLink:
    name: str
    t_size: int
    hash: bytes


class RawPBNode:
    data: bytes
    links: list[RawPBLink]
