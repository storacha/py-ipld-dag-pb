from .node import PBNode
from .encode import encode_node

name = "dag-pb"
code = 0x70

/**
 * @param {PBNode} node
 * @returns {ByteView<PBNode>}
 */
def encode(node: PBNode) -> bytes {
  validate(node)

  const pbn = {}
  if (node.Links) {
    pbn.Links = node.Links.map((l) => {
      const link = {}
      if (l.Hash) {
        link.Hash = l.Hash.bytes // cid -> bytes
      }
      if (l.Name !== undefined) {
        link.Name = l.Name
      }
      if (l.Tsize !== undefined) {
        link.Tsize = l.Tsize
      }
      return link
    })
  }
  if (node.Data) {
    pbn.Data = node.Data
  }

  return encodeNode(pbn)
}

# /**
#  * @param {ByteView<PBNode> | ArrayBufferView<PBNode>} bytes
#  * @returns {PBNode}
#  */
# export function decode (bytes) {
#   const buf = toByteView(bytes)
#   const pbn = decodeNode(buf)

#   const node = {}

#   if (pbn.Data) {
#     node.Data = pbn.Data
#   }

#   if (pbn.Links) {
#     node.Links = pbn.Links.map((l) => {
#       const link = {}
#       try {
#         link.Hash = CID.decode(l.Hash)
#       } catch (e) {}
#       if (!link.Hash) {
#         throw new Error('Invalid Hash field found in link, expected CID')
#       }
#       if (l.Name !== undefined) {
#         link.Name = l.Name
#       }
#       if (l.Tsize !== undefined) {
#         link.Tsize = l.Tsize
#       }
#       return link
#     })
#   }

#   return node
# }