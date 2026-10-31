// Near-duplicate evidence detection via signed feature-hash cosine (Bend kernel).
type VecRec:
  id: String
  features: [Float]

def dot(a: [Float], b: [Float]):
  return sum(map(zip(a, b), lambda pair: pair.0 * pair.1))

def cosine(a: VecRec, b: VecRec):
  return dot(a.features, b.features) / (sqrt(dot(a.features,a.features)) * sqrt(dot(b.features,b.features)) + 1e-9)

def dedup(vectors: [VecRec]):
  // keep first of any pair with cosine >= 0.92
  return filter_with_index(vectors, lambda v, i:
    all(map_with_index(vectors, lambda u, j: (j >= i) or (cosine(u, v) < 0.92))))
