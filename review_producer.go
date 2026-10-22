package main

// reviewProducerFault is a transaction-failure seam, analogous to
// nativeWriterHandoverFault. Production leaves it nil. It supplies neither a
// reviewer nor review-origin evidence; no accepting producer is implemented here.
var reviewProducerFault func(stage string) error
