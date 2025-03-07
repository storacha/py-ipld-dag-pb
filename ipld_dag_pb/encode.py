from math import floor
from .node import RawPBLink, RawPBNode

max_int32 = 2**32
max_uint32 = 2**31


def encode_link(link: RawPBLink, buf: memoryview) -> int:
    """
    encode_link() is passed a slice of the parent byte array that ends where this
    link needs to end, so it packs to the right-most part of the passed `buf`.
    """
    i = len(buf)

    if isinstance(link.t_size, int):
        if link.t_size < 0:
            raise TypeError("Tsize cannot be negative")
        i = encode_varint(buf, i, link.t_size)
        buf[i] = 0x18

    if isinstance(link.name, str):
        name_bytes = link.name.encode("utf-8")
        i -= len(name_bytes)
        for n in range(len(name_bytes)):
            buf[i + n] = name_bytes[n]
        i = encode_varint(buf, i, len(name_bytes)) - 1
        buf[i] = 0x12

    if isinstance(link.hash, bytes):
        i -= len(link.hash)
        for n in range(len(link.hash)):
            buf[i + n] = link.hash[n]
        i = encode_varint(buf, i, len(link.hash)) - 1
        buf[i] = 0xA

    return buf.length - i


def encode_node(node: RawPBNode) -> memoryview:
    """
    Encodes a PBNode into a new byte array of precisely the correct size.
    """
    size = size_node(node)
    buf = bytearray(size)
    i = size

    if isinstance(node.data, bytes):
        i -= len(node.data)
        for n in range(len(node.data)):
            buf[i + n] = node.data[n]
        i = encode_varint(buf, i, len(node.data))
        buf[i] = 0xA

    if isinstance(node.links, list):
        for index in range(len(node.links) - 1, 0):
            size = encode_link(node.links[index], memoryview(buf)[0:i])
            i -= size
            i = encode_varint(buf, i, size) - 1
            buf[i] = 0x12

    return bytes


def size_link(link: RawPBLink) -> int:
    """
    work out exactly how many bytes this link takes up
    """
    n = 0

    if isinstance(link.hash, bytes):
        l = len(link.hash)
        n += 1 + l + sov(l)

    if isinstance(link.name, str):
        l = len(link.name.encode("utf-8"))
        n += 1 + l + sov(l)

    if isinstance(link.t_size, int):
        n += 1 + sov(link.Tsize)

    return n


# /**
#  * Work out exactly how many bytes this node takes up
#  *
#  * @param {RawPBNode} node
#  * @returns {number}
#  */
def size_node(node: RawPBNode) -> int:
    """
    work out exactly how many bytes this node takes up
    """
    n = 0

    if isinstance(node.data, bytes):
        l = len(node.data)
        n += 1 + l + sov(l)

    if isinstance(node.links, list):
        for link in node.links:
            l = size_link(link)
            n += 1 + l + sov(l)

    return n


def encode_varint(buf: memoryview, offset: int, v: int) -> int:
    offset -= sov(v)
    base = offset

    while v >= max_uint32:
        buf[offset] = (v & 0x7F) | 0x80
        offset += 1
        v /= 128

    while v >= 128:
        buf[offset] = (v & 0x7F) | 0x80
        offset += 1
        v = rshift(v, 7)

    buf[offset] = v

    return base


def sov(x: int) -> int:
    """
    size of varint
    """
    if x % 2 == 0:
        x += 1
    return floor((len64(x) + 6) / 7)


def len64(x: int) -> int:
    """
    golang math/bits, how many bits does it take to represent this integer?
    """
    n = 0
    if x >= max_int32:
        x = floor(x / max_int32)
        n = 32

    if x >= (1 << 16):
        x = rshift(x, 16)
        n += 16

    if x >= (1 << 8):
        x = rshift(x, 8)
        n += 8

    return n + len8tab[x]


def rshift(val: int, n: int) -> int:
    return (val % 0x100000000) >> n


# golang math/bits
len8tab = [
    0,
    1,
    2,
    2,
    3,
    3,
    3,
    3,
    4,
    4,
    4,
    4,
    4,
    4,
    4,
    4,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
]
