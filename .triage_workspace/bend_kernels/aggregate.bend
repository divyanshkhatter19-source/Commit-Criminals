// Parallel evidence-confidence fold (Bend kernel).
// Semantics must match python_fallback_aggregate() below exactly.
type ClaimRec:
  claim_id: String
  weights: (Float, Bool)   // (confidence_contribution, is_contradicting)

def fold_step(acc: Float, pair: (Float, Bool)):
  (w, contra) = pair
  if contra:
    return max(0.0, acc - 0.8 * w)
  return acc + (1.0 - acc) * w

def aggregate(evidence: (Float, Bool)):
  return fold(evidence, 0.0, fold_step)
