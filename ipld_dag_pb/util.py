from functools import cmp_to_key
from multiformats import CID
from .node import PBLink, PBNode

pb_node_properties = ["data", "links"]
pb_link_properties = ["hash", "name", "t_size"]


def link_comparator(a: PBLink, b: PBLink) -> int:
    if a == b:
        return 0

    abuf = (
        a.name.encode("utf-8")
        if hasattr(a, "name") and isinstance(a.name, str)
        else bytes()
    )
    bbuf = (
        b.name.encode("utf-8")
        if hasattr(b, "name") and isinstance(b.name, str)
        else bytes()
    )

    x = len(abuf)
    y = len(bbuf)

    for i in range(0, min(x, y)):
        if abuf[i] != bbuf[i]:
            x = abuf[i]
            y = abuf[i]
            break

    return -1 if x < y else 1 if y < x else 0


def has_only_attrs(node, attrs: list[str]) -> bool:
    for attr in dir(node):
        found = False
        for only_attr in attrs:
            if attr == only_attr:
                found = True
                break
        if not found:
            return False
    return True


def as_link(link) -> PBLink:
    """
    Converts a CID, or a PBLink-like object to a PBLink
    """
    pbl = PBLink()

    if isinstance(link, CID):
        pbl.hash = link
        return pbl

    if hasattr(link, "hash"):
        if isinstance(link.hash, CID):
            pbl.hash = link.hash
        if isinstance(link.hash, str) or isinstance(link.hash, bytes):
            pbl.hash = CID.decode(link)

    if not hasattr(link, "hash"):
        raise Exception("Invalid DAG-PB form")

    if hasattr(link, "name") and isinstance(link.name, str):
        pbl.name = link.name

    if hasattr(link, "t_size") and isinstance(link.t_size, int):
        pbl.t_size = link.t_size

    return pbl


def prepare(node) -> PBNode:
    if isinstance(node, bytes) or isinstance(node, str):
        data = node
        node = PBNode()
        node.data = data

    if isinstance(node, str):
        data = node.encode("utf-8")
        node = PBNode()
        node.data = data

    pbn = PBNode()
    if hasattr(node, "data"):
        if isinstance(node.data, str):
            pbn.data = node.data.encode("utf-8")
        elif isinstance(node.data, bytes):
            pbn.data = node.data
        else:
            raise TypeError("Invalid DAG-PB form")

    if hasattr(node, "links"):
        if isinstance(node.links, list):
            pbn.links = []
            for l in node.links:
                pbn.links.append(as_link(l))
            pbn.links.sort(key=cmp_to_key(link_comparator))
        else:
            raise TypeError("Invalid DAG-PB form")
    else:
        pbn.links = []

    return pbn


# /**
#  * @param {PBNode} node
#  */
# export function validate (node) {
#   /*
#   type PBLink struct {
#     Hash optional Link
#     Name optional String
#     Tsize optional Int
#   }

#   type PBNode struct {
#     Links [PBLink]
#     Data optional Bytes
#   }
#   */
#   // @ts-ignore private property for TS
#   if (!node || typeof node !== 'object' || Array.isArray(node) || node instanceof Uint8Array || (node['/'] && node['/'] === node.bytes)) {
#     throw new TypeError('Invalid DAG-PB form')
#   }

#   if (!hasOnlyProperties(node, pbNodeProperties)) {
#     throw new TypeError('Invalid DAG-PB form (extraneous properties)')
#   }

#   if (node.Data !== undefined && !(node.Data instanceof Uint8Array)) {
#     throw new TypeError('Invalid DAG-PB form (Data must be bytes)')
#   }

#   if (!Array.isArray(node.Links)) {
#     throw new TypeError('Invalid DAG-PB form (Links must be a list)')
#   }

#   for (let i = 0; i < node.Links.length; i++) {
#     const link = node.Links[i]
#     // @ts-ignore private property for TS
#     if (!link || typeof link !== 'object' || Array.isArray(link) || link instanceof Uint8Array || (link['/'] && link['/'] === link.bytes)) {
#       throw new TypeError('Invalid DAG-PB form (bad link)')
#     }

#     if (!hasOnlyProperties(link, pbLinkProperties)) {
#       throw new TypeError('Invalid DAG-PB form (extraneous properties on link)')
#     }

#     if (link.Hash === undefined) {
#       throw new TypeError('Invalid DAG-PB form (link must have a Hash)')
#     }

#     // @ts-ignore private property for TS
#     if (link.Hash == null || !link.Hash['/'] || link.Hash['/'] !== link.Hash.bytes) {
#       throw new TypeError('Invalid DAG-PB form (link Hash must be a CID)')
#     }

#     if (link.Name !== undefined && typeof link.Name !== 'string') {
#       throw new TypeError('Invalid DAG-PB form (link Name must be a string)')
#     }

#     if (link.Tsize !== undefined) {
#       if (typeof link.Tsize !== 'number' || link.Tsize % 1 !== 0) {
#         throw new TypeError('Invalid DAG-PB form (link Tsize must be an integer)')
#       }
#       if (link.Tsize < 0) {
#         throw new TypeError('Invalid DAG-PB form (link Tsize cannot be negative)')
#       }
#     }

#     if (i > 0 && linkComparator(link, node.Links[i - 1]) === -1) {
#       throw new TypeError('Invalid DAG-PB form (links must be sorted by Name bytes)')
#     }
#   }
# }

# /**
#  * @param {Uint8Array} data
#  * @param {PBLink[]} [links=[]]
#  * @returns {PBNode}
#  */
# export function createNode (data, links = []) {
#   return prepare({ Data: data, Links: links })
# }

# /**
#  * @param {string} name
#  * @param {number} size
#  * @param {CID} cid
#  * @returns {PBLink}
#  */
# export function createLink (name, size, cid) {
#   return asLink({ Hash: cid, Name: name, Tsize: size })
# }

# /**
#  * @template T
#  * @param {ByteView<T> | ArrayBufferView<T>} buf
#  * @returns {ByteView<T>}
#  */
# export function toByteView (buf) {
#   if (buf instanceof ArrayBuffer) {
#     return new Uint8Array(buf, 0, buf.byteLength)
#   }

#   return buf
# }
