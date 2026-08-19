import { toAttemptId } from '@shared/ai/attempt'
import { describe, expect, it } from 'vitest'

import { TopicStreamAggregate } from '../TopicStreamAggregate'
import { toContinuationLeaseId } from '../topicStreamState'

const lease = toContinuationLeaseId('continuation-1')

describe('TopicStreamAggregate', () => {
  it('does not quiesce while any admitted attempt is still finalizing', () => {
    const aggregate = new TopicStreamAggregate('topic-1')
    const slow = aggregate.reserveAttempt(toAttemptId(1))
    const fast = aggregate.reserveAttempt(toAttemptId(2))

    aggregate.transitionAttempt(slow.id, { type: 'launch' })
    aggregate.transitionAttempt(fast.id, { type: 'launch' })
    aggregate.transitionAttempt(slow.id, { type: 'complete' })
    aggregate.transitionAttempt(fast.id, { type: 'complete' })
    aggregate.transitionAttempt(fast.id, { type: 'persisted' })

    expect(aggregate.isQuiescent()).toBe(false)
    expect(aggregate.status()).toBe('pending')

    aggregate.transitionAttempt(slow.id, { type: 'persisted' })

    expect(aggregate.isQuiescent()).toBe(true)
    expect(aggregate.status()).toBe('done')
    expect(aggregate.attemptWatermark()).toBe(fast.id)
  })

  it('settles an awaiting approval attempt and reserves its continuation in one commit', () => {
    const aggregate = new TopicStreamAggregate('topic-1')
    const attempt = aggregate.reserveAttempt(toAttemptId(1))

    aggregate.transitionAttempt(attempt.id, { type: 'launch' })
    aggregate.setApprovalPending(attempt.id, 'tool-1', true)
    aggregate.transitionAttempt(attempt.id, { type: 'complete' })
    aggregate.transitionAttempt(attempt.id, { type: 'approval-persisted' })

    expect(aggregate.attemptState(attempt.id)?.phase).toBe('awaiting-approval')
    expect(aggregate.isQuiescent()).toBe(false)
    expect(aggregate.status()).toBe('awaiting-approval')

    const continuationId = toAttemptId(2)
    const prepared = aggregate.prepare({
      type: 'reserve-dispatch',
      attemptIds: [continuationId],
      reservation: { kind: 'approval-resume', attemptId: attempt.id }
    })
    aggregate.commit(prepared)

    expect(aggregate.attemptState(attempt.id)).toMatchObject({ phase: 'settled', outcome: { kind: 'done' } })
    expect(aggregate.attempt(attempt.id)?.pendingApprovalToolCallIds.size).toBe(0)
    expect(aggregate.attemptState(continuationId)).toEqual({ phase: 'reserved' })
    expect(aggregate.isQuiescent()).toBe(false)
  })

  it('stops one awaiting-approval branch without changing settled siblings', () => {
    const aggregate = new TopicStreamAggregate('topic-1')
    const settled = aggregate.reserveAttempt(toAttemptId(1))
    const awaiting = aggregate.reserveAttempt(toAttemptId(2))
    for (const attempt of [settled, awaiting]) {
      aggregate.transitionAttempt(attempt.id, { type: 'launch' })
      aggregate.transitionAttempt(attempt.id, { type: 'complete' })
    }
    aggregate.transitionAttempt(settled.id, { type: 'persisted' })
    aggregate.setApprovalPending(awaiting.id, 'tool-1', true)
    aggregate.transitionAttempt(awaiting.id, { type: 'approval-persisted' })

    const stopped = aggregate.stop('user-requested')

    expect(aggregate.attemptState(settled.id)).toMatchObject({ phase: 'settled', outcome: { kind: 'done' } })
    expect(aggregate.attemptState(awaiting.id)).toMatchObject({
      phase: 'finalizing',
      outcome: { kind: 'aborted', reason: 'user-requested' }
    })
    expect(stopped.effects).toContainEqual({
      type: 'stop-attempt',
      attemptId: awaiting.id,
      priorPhase: 'awaiting-approval',
      reason: 'user-requested'
    })
  })

  it('owns chat steer FIFO and consumes only the exact queue head', () => {
    const aggregate = new TopicStreamAggregate('topic-1')
    const firstLease = toContinuationLeaseId('steer-1')
    const secondLease = toContinuationLeaseId('steer-2')
    aggregate.enqueueChatSteer({ id: 'first', leaseId: firstLease, userMessageId: 'u1', fastMode: false })
    aggregate.enqueueChatSteer({ id: 'second', leaseId: secondLease, userMessageId: 'u2', fastMode: true })

    const wrongHead = aggregate.prepare({
      type: 'reserve-dispatch',
      attemptIds: [toAttemptId(1)],
      reservation: { kind: 'continuation', leaseId: secondLease, chatSteerId: 'second' }
    })
    expect(wrongHead.rejection).toBe('invalid-continuation')

    aggregate.commit(
      aggregate.prepare({
        type: 'reserve-dispatch',
        attemptIds: [toAttemptId(1)],
        reservation: { kind: 'continuation', leaseId: firstLease, chatSteerId: 'first' }
      })
    )
    expect(aggregate.pendingChatSteers().map((steer) => steer.id)).toEqual(['second'])
    expect(aggregate.continuationLease(firstLease)).toMatchObject({ state: 'consumed', attemptId: toAttemptId(1) })

    aggregate.dropChatSteers('queue-cleared')
    expect(aggregate.pendingChatSteers()).toEqual([])
    expect(aggregate.continuationLease(secondLease)).toMatchObject({ state: 'released', reason: 'queue-cleared' })
  })

  it('requires and consumes fresh Agent ownership while Stop preserves an open runtime owner', () => {
    const aggregate = new TopicStreamAggregate('agent-session:session-1')
    const ownership = toContinuationLeaseId('runtime-owner-1')
    aggregate.openContinuationLease(ownership, 'agent-runtime', false, 'runtime-ownership')

    const attemptId = toAttemptId(1)
    aggregate.commit(
      aggregate.prepare({
        type: 'reserve-dispatch',
        attemptIds: [attemptId],
        reservation: { kind: 'fresh', ownershipLeaseId: ownership }
      })
    )
    expect(aggregate.continuationLease(ownership)).toMatchObject({ state: 'consumed', attemptId })

    const terminalOwner = toContinuationLeaseId('runtime-owner-terminal')
    aggregate.openContinuationLease(terminalOwner, 'agent-runtime', false, 'runtime-ownership')
    aggregate.stop('user-requested')
    expect(aggregate.continuationLease(terminalOwner)).toMatchObject({ state: 'open', kind: 'runtime-ownership' })
    expect(aggregate.isQuiescent()).toBe(false)
  })

  it('does not quiesce when neither the final projection nor an error marker is durable', () => {
    const aggregate = new TopicStreamAggregate('topic-1')
    const attempt = aggregate.reserveAttempt(toAttemptId(1))

    aggregate.transitionAttempt(attempt.id, { type: 'launch' })
    aggregate.transitionAttempt(attempt.id, { type: 'complete' })
    aggregate.transitionAttempt(attempt.id, {
      type: 'persist-failed',
      error: { name: 'Error', message: 'db unavailable', stack: null },
      durableErrorWritten: false
    })

    expect(aggregate.attemptState(attempt.id)?.phase).toBe('persistence-blocked')
    expect(aggregate.areAttemptsDurablySettled()).toBe(false)
    expect(aggregate.isQuiescent()).toBe(false)
  })

  it('keeps a durable topic open until its continuation is consumed', () => {
    const aggregate = new TopicStreamAggregate('topic-1')
    const attempt = aggregate.reserveAttempt(toAttemptId(1))
    aggregate.transitionAttempt(attempt.id, { type: 'launch' })
    aggregate.openContinuationLease(lease, 'chat-steer')
    aggregate.transitionAttempt(attempt.id, { type: 'complete' })
    aggregate.transitionAttempt(attempt.id, { type: 'persisted' })

    expect(aggregate.areAttemptsDurablySettled()).toBe(true)
    expect(aggregate.isQuiescent()).toBe(false)
    expect(aggregate.status()).toBe('streaming')

    expect(aggregate.consumeContinuationLease(lease, attempt.id)).toBe(true)

    expect(aggregate.isQuiescent()).toBe(true)
    expect(aggregate.status()).toBe('done')
  })

  it('turns a failed continuation into a quiescent error outcome', () => {
    const aggregate = new TopicStreamAggregate('topic-1')
    const attempt = aggregate.reserveAttempt(toAttemptId(1))
    aggregate.transitionAttempt(attempt.id, { type: 'launch' })
    aggregate.transitionAttempt(attempt.id, { type: 'complete' })
    aggregate.transitionAttempt(attempt.id, { type: 'persisted' })
    aggregate.openContinuationLease(lease, 'agent-runtime')

    aggregate.releaseContinuationLease(lease, 'source-error')

    expect(aggregate.isQuiescent()).toBe(true)
    expect(aggregate.status()).toBe('error')
  })

  it('settles a lease exactly once: the first terminal transition wins (L2)', () => {
    const aggregate = new TopicStreamAggregate('topic-1')
    const attempt = aggregate.reserveAttempt(toAttemptId(1))
    aggregate.transitionAttempt(attempt.id, { type: 'launch' })
    aggregate.transitionAttempt(attempt.id, { type: 'complete' })
    aggregate.transitionAttempt(attempt.id, { type: 'persisted' })
    aggregate.openContinuationLease(lease, 'chat-steer')

    expect(aggregate.consumeContinuationLease(lease, attempt.id)).toBe(true)
    expect(aggregate.releaseContinuationLease(lease, 'stop')).toBe(false)
    expect(aggregate.continuationLease(lease)?.state).toBe('consumed')
    // A settled lease cannot reopen the topic.
    expect(aggregate.isQuiescent()).toBe(true)
    expect(aggregate.status()).toBe('done')
  })

  it('reserves a continuation attempt and consumes its exact lease in one commit', () => {
    const aggregate = new TopicStreamAggregate('topic-1')
    aggregate.openContinuationLease(lease, 'agent-runtime')
    const attemptId = toAttemptId(2)

    const prepared = aggregate.prepare({
      type: 'reserve-dispatch',
      attemptIds: [attemptId],
      reservation: { kind: 'continuation', leaseId: lease }
    })
    aggregate.commit(prepared)

    expect(aggregate.attemptState(attemptId)).toEqual({ phase: 'reserved' })
    expect(aggregate.continuationLease(lease)).toMatchObject({ state: 'consumed', attemptId })
  })

  it('structurally rejects a fresh reservation while an attempt is finalizing', () => {
    const aggregate = new TopicStreamAggregate('topic-1')
    const current = aggregate.reserveAttempt(toAttemptId(1))
    aggregate.transitionAttempt(current.id, { type: 'launch' })
    aggregate.transitionAttempt(current.id, { type: 'complete' })

    const nextAttemptId = toAttemptId(2)
    const prepared = aggregate.prepare({
      type: 'reserve-dispatch',
      attemptIds: [nextAttemptId],
      reservation: { kind: 'fresh' }
    })

    expect(prepared.rejection).toBe('busy')
    expect(prepared.changed).toBe(false)
    expect(aggregate.attemptState(nextAttemptId)).toBeUndefined()
  })

  it('pushes ring eviction pause on the approval edges, never on inner changes (T8)', () => {
    const aggregate = new TopicStreamAggregate('topic-1')
    const pushed: Array<{ attemptId: number; paused: boolean }> = []
    aggregate.setFlagEffectSink((effect) => pushed.push({ attemptId: effect.attemptId, paused: effect.paused }))
    const attempt = aggregate.reserveAttempt(toAttemptId(1))
    aggregate.transitionAttempt(attempt.id, { type: 'launch' })

    aggregate.setApprovalPending(attempt.id, 'tool-1', true)
    aggregate.setApprovalPending(attempt.id, 'tool-2', true)
    aggregate.setApprovalPending(attempt.id, 'tool-1', false)
    aggregate.setApprovalPending(attempt.id, 'tool-2', false)

    // Only the empty↔non-empty edges flip the ring; a parallel approval must not resume eviction.
    expect(pushed).toEqual([
      { attemptId: attempt.id, paused: true },
      { attemptId: attempt.id, paused: false }
    ])
  })
})
