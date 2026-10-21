package main

// embedding.go holds the spec-graph-specific embedding-text
// canonicalization helper. The Vertex AI transport itself lives in
// internal/vertexai (per Vision Principle 12 matryoshka: one Vertex AI
// client surface, reused by every consumer in this repo).

// canonicalEmbedText is the text spec-graph submits to Vertex AI at
// admission time. Deterministic over (node_kind, stable_id, contract,
// payload) so the same node embeds identically across runs.
func canonicalEmbedText(sub ArtifactSubmission) (string, error) {
	canon := map[string]interface{}{
		"node_kind": sub.NodeKind,
		"stable_id": sub.StableID,
		"contract":  sub.Contract,
		"payload":   sub.Payload,
	}
	b, err := canonicalJSON(canon)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
