package dataplane

import (
	"time"

	"github.com/junjiefly/kache-connector/internal/errs"
	kachev1 "github.com/junjiefly/kache-connector/proto/kache/v1"
)

type factoryLookup func(transport string) (Factory, bool)

func modeAllows(mode, transport string) bool {
	switch mode {
	case "auto":
		return transport == "rdma" || transport == "tcp"
	case "rdma", "tcp":
		return transport == mode
	default:
		return false
	}
}

// validateWritePlan enforces every write-plan rule before any data-plane write.
func validateWritePlan(reply *kachev1.PreparePutBatchReply, chunks []*kachev1.PutChunk, lim Limits, mode string, lookup factoryLookup, now time.Time) error {
	if !reply.GetAccepted() {
		return errs.New(errs.InvalidWritePlan, "prepare reply not accepted")
	}
	if reply.GetPutId() == "" {
		return errs.New(errs.InvalidWritePlan, "empty put_id")
	}
	if exp := reply.GetExpiresAtUnixSec(); exp != 0 && exp <= now.Unix() {
		return errs.New(errs.PlanExpired, "write plan expired at %d", exp)
	}
	seen := map[string]bool{}
	descByChunk := map[uint32][]*kachev1.PhysicalBlock{}
	byChunk := map[uint32][]*kachev1.PhysicalBlock{}
	for _, b := range reply.GetBlocks() {
		id := b.GetPlanBlockId()
		if id == "" || seen[id] {
			return errs.New(errs.InvalidWritePlan, "empty or duplicate plan_block_id")
		}
		seen[id] = true
		if int(b.GetChunkIndex()) >= len(chunks) {
			return errs.New(errs.InvalidWritePlan, "chunk_index %d out of range", b.GetChunkIndex())
		}
		if err := validateBlock(b, lim, lookup, true); err != nil {
			return err
		}
		if !hasAllowedTarget(b, mode) {
			return errs.New(errs.InvalidWritePlan, "block %s has no target allowed by transport mode %q", id, mode)
		}
		if b.GetKind() == kachev1.BlockKind_BLOCK_KIND_DESC {
			descByChunk[b.GetChunkIndex()] = append(descByChunk[b.GetChunkIndex()], b)
		} else {
			byChunk[b.GetChunkIndex()] = append(byChunk[b.GetChunkIndex()], b)
		}
	}
	for i, ch := range chunks {
		if err := validateCoverage(byChunk[uint32(i)], ch.GetSizeBytes()); err != nil {
			return errs.New(errs.InvalidWritePlan, "chunk %d (%s): %v", i, ch.GetKey(), err)
		}
		if desc := descByChunk[uint32(i)]; len(desc) > 1 {
			return errs.New(errs.InvalidWritePlan, "chunk %d has multiple desc blocks", i)
		}
	}
	return nil
}

// validateReadPlan enforces the read-plan coverage rules.
func validateReadPlan(plan *kachev1.ReadPlan, lim Limits, lookup factoryLookup) error {
	if plan.GetSizeBytes() == 0 {
		return errs.New(errs.InvalidReadPlan, "zero size_bytes")
	}
	for _, b := range plan.GetBlocks() {
		if b.GetKind() == kachev1.BlockKind_BLOCK_KIND_DESC {
			return errs.New(errs.InvalidReadPlan, "read plan cannot contain desc block")
		}
		if b.GetPlanBlockId() == "" {
			return errs.New(errs.InvalidReadPlan, "empty plan_block_id")
		}
		if err := validateBlock(b, lim, lookup, false); err != nil {
			return errs.Wrap(errs.InvalidReadPlan, err, "block %s", b.GetPlanBlockId())
		}
	}
	if err := validateCoverage(plan.GetBlocks(), plan.GetSizeBytes()); err != nil {
		return errs.Wrap(errs.InvalidReadPlan, err, "key %s", plan.GetKey())
	}
	if h := plan.GetContentHash(); len(h) != 0 && len(h) != 32 {
		return errs.New(errs.InvalidReadPlan, "content_hash must be 32 bytes")
	}
	return nil
}

func validateBlock(b *kachev1.PhysicalBlock, lim Limits, lookup factoryLookup, requireKind bool) error {
	if requireKind && b.GetKind() == kachev1.BlockKind_BLOCK_KIND_UNSPECIFIED {
		return errs.New(errs.InvalidWritePlan, "block kind unspecified")
	}
	if b.GetKind() == kachev1.BlockKind_BLOCK_KIND_DESC {
		if len(b.GetPayload()) == 0 {
			return errs.New(errs.InvalidWritePlan, "desc block payload is empty")
		}
		if b.GetLogicalLen() != uint32(len(b.GetPayload())) || b.GetWireLen() != b.GetLogicalLen() {
			return errs.New(errs.InvalidWritePlan, "desc block logical/wire len must equal payload length %d", len(b.GetPayload()))
		}
	} else {
		if b.GetLogicalLen() == 0 {
			return errs.New(errs.InvalidWritePlan, "zero logical_len")
		}
		if b.GetWireLen() < b.GetLogicalLen() || b.GetWireLen() == 0 {
			return errs.New(errs.InvalidWritePlan, "wire_len %d invalid for logical_len %d", b.GetWireLen(), b.GetLogicalLen())
		}
	}
	if len(b.GetOpaqueRef()) == 0 {
		return errs.New(errs.InvalidWritePlan, "empty opaque_ref")
	}
	if b.GetRefKind() == "" {
		return errs.New(errs.UnsupportedRefKind, "empty ref_kind")
	}
	if len(b.GetTargets()) == 0 {
		return errs.New(errs.InvalidWritePlan, "block has no targets")
	}
	ids := map[string]bool{}
	for _, t := range b.GetTargets() {
		if t.GetTargetId() == "" || ids[t.GetTargetId()] {
			return errs.New(errs.InvalidWritePlan, "empty or duplicate target_id")
		}
		ids[t.GetTargetId()] = true
		if t.GetEndpoint() == "" || len(t.GetCapability()) == 0 {
			return errs.New(errs.InvalidWritePlan, "target missing endpoint or capability")
		}
		f, ok := lookup(t.GetTransport())
		if !ok {
			return errs.New(errs.TransportUnavailable, "unsupported transport %q", t.GetTransport())
		}
		if !f.SupportsRefKind(b.GetRefKind()) {
			return errs.New(errs.UnsupportedRefKind, "ref_kind %q not supported by %s backend", b.GetRefKind(), t.GetTransport())
		}
		maxWire := f.MaxBlockBytes()
		if lim.MaxWireBlockBytes > 0 && lim.MaxWireBlockBytes < maxWire {
			maxWire = lim.MaxWireBlockBytes
		}
		if b.GetWireLen() > maxWire {
			return errs.New(errs.InvalidWritePlan, "wire_len %d exceeds max wire block bytes %d", b.GetWireLen(), maxWire)
		}
	}
	return nil
}

func hasAllowedTarget(b *kachev1.PhysicalBlock, mode string) bool {
	for _, t := range b.GetTargets() {
		if modeAllows(mode, t.GetTransport()) {
			return true
		}
	}
	return false
}

// validateCoverage requires ordered, gap-free, non-overlapping blocks whose
// logical lengths sum exactly to the chunk size. Desc blocks are skipped
// because their chunk_offset is at end-of-data and their logical_len is the
// metadata payload length, not data coverage.
func validateCoverage(blocks []*kachev1.PhysicalBlock, size uint64) error {
	var data []*kachev1.PhysicalBlock
	for _, b := range blocks {
		if b.GetKind() != kachev1.BlockKind_BLOCK_KIND_DESC {
			data = append(data, b)
		}
	}
	if len(data) == 0 {
		return errs.New(errs.InvalidWritePlan, "no blocks")
	}
	var expected uint64
	for _, b := range data {
		if b.GetChunkOffset() != expected {
			return errs.New(errs.InvalidWritePlan, "block offset %d, expected %d (gap, overlap or unsorted)", b.GetChunkOffset(), expected)
		}
		expected += uint64(b.GetLogicalLen())
	}
	if expected != size {
		return errs.New(errs.InvalidWritePlan, "logical coverage %d != size_bytes %d", expected, size)
	}
	return nil
}
