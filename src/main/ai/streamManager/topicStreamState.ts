import type { AttemptId } from '@shared/ai/attempt'
import type { TopicStreamStatus } from '@shared/ai/transport'
import type { ReasoningEffortOption } from '@shared/types/aiSdk'
import type { SerializedError } from '@shared/types/error'

import {
  type AttemptEvent,
  type AttemptOutcome,
  type AttemptState,
  reduceTopicStatus,
  type StreamLifecycleState,
  transition
} from './attemptMachine'

export type ContinuationLeaseId = string & { readonly __continuationLeaseId: unique symbol }

export const toContinuationLeaseId = (value: string): ContinuationLeaseId => value as ContinuationLeaseId

export type ContinuationReleaseReason = 'stop' | 'queue-cleared' | 'launch-failed' | 'handoff-rejected' | 'source-error'

interface TopicContinuationLeaseBase {
  readonly id: ContinuationLeaseId
  readonly topicId: string
  readonly cycleId: number
  /**
   * The promised work only exists if the attempt that opened the lease succeeds — a steer
   * transition or compaction resume, as opposed to independently queued or deferred work.
   * Structural: the reducer voids these itself when an attempt lands on an error terminal, so
   * nobody has to ask the opener afterwards whether it still intends to continue.
   */
  readonly voidOnAttemptError: boolean
  /** Diagnostics only. Reducers and selectors must not branch on this field. */
  readonly diagnosticOwner: 'agent-runtime' | 'chat-steer'
  /** Continuations promise a future attempt; runtime ownership holds a row/buffer until handoff
   *  or terminal persistence. Stop releases only the former. */
  readonly kind: 'continuation' | 'runtime-ownership'
}

export type TopicContinuationLease =
  | (TopicContinuationLeaseBase & { readonly state: 'open' })
  | (TopicContinuationLeaseBase & { readonly state: 'consumed'; readonly attemptId: AttemptId })
  | (TopicContinuationLeaseBase & { readonly state: 'released'; readonly reason: ContinuationReleaseReason })

export interface TopicAttemptState {
  readonly id: AttemptId
  readonly state: AttemptState
  readonly pendingApprovalToolCallIds: ReadonlySet<string>
}

export interface TopicStreamState {
  readonly topicId: string
  readonly cycleId: number
  readonly revision: number
  readonly lifecycle: StreamLifecycleState
  readonly attempts: ReadonlyMap<AttemptId, TopicAttemptState>
  readonly continuationLeases: ReadonlyMap<ContinuationLeaseId, TopicContinuationLease>
  readonly pendingChatSteers: readonly PendingChatSteer[]
}

export interface PendingChatSteer {
  readonly id: string
  readonly leaseId: ContinuationLeaseId
  readonly userMessageId: string
  readonly reasoningEffort?: ReasoningEffortOption
  readonly fastMode: boolean
}

export type TopicDispatchReservation =
  | { readonly kind: 'fresh'; readonly ownershipLeaseId?: ContinuationLeaseId }
  | { readonly kind: 'live-change' }
  | { readonly kind: 'approval-resume'; readonly attemptId: AttemptId }
  | {
      readonly kind: 'continuation'
      readonly leaseId: ContinuationLeaseId
      readonly chatSteerId?: string
      readonly ownershipLeaseId?: ContinuationLeaseId
    }

export type TopicStreamCommand =
  /** Reserves every attempt of one dispatch at a single revision, or none of them (T2). */
  | { type: 'reserve-dispatch'; attemptIds: readonly AttemptId[]; reservation: TopicDispatchReservation }
  | { type: 'reserve-attempt'; attemptId: AttemptId }
  | { type: 'forget-attempt'; attemptId: AttemptId }
  | { type: 'attempt-event'; attemptId: AttemptId; event: AttemptEvent }
  | { type: 'approval-changed'; attemptId: AttemptId; toolCallId: string; pending: boolean }
  | { type: 'approvals-cleared'; attemptId: AttemptId }
  | { type: 'enqueue-chat-steer'; steer: PendingChatSteer }
  | { type: 'drop-chat-steers'; reason: ContinuationReleaseReason }
  | { type: 'stop-topic'; reason: string }
  | { type: 'dispatch-preparation-failed'; attemptIds: readonly AttemptId[]; error: SerializedError }
  | {
      type: 'continuation-opened'
      leaseId: ContinuationLeaseId
      diagnosticOwner: 'agent-runtime' | 'chat-steer'
      kind?: 'continuation' | 'runtime-ownership'
      voidOnAttemptError?: boolean
    }
  | { type: 'continuation-updated'; leaseId: ContinuationLeaseId; voidOnAttemptError: boolean }
  | { type: 'continuation-consumed'; leaseId: ContinuationLeaseId; attemptId: AttemptId }
  | { type: 'continuation-released'; leaseId: ContinuationLeaseId; reason: ContinuationReleaseReason }
  | { type: 'activate' }
  | { type: 'begin-grace' }
  | { type: 'evict' }
  | { type: 'touch' }

/** Ephemeral normalized reducer facts. They are never appended or replayed. */
export type TopicStreamEvent =
  | { type: 'attempt-reserved'; attemptId: AttemptId }
  | { type: 'attempt-changed'; attemptId: AttemptId; state: AttemptState }
  | { type: 'approval-changed'; attemptId: AttemptId; pending: boolean }
  | { type: 'continuation-changed'; lease: TopicContinuationLease }
  | { type: 'lifecycle-changed'; lifecycle: StreamLifecycleState }

/**
 * Resource-flag effects are applied synchronously by the executor before its first `await` (T8).
 * They push reducer state onto a resource so the resource never reads the reducer back.
 */
export type TopicStreamFlagEffect = { type: 'set-ring-eviction'; attemptId: AttemptId; paused: boolean }

export type TopicStreamWorkEffect = {
  type: 'stop-attempt'
  attemptId: AttemptId
  priorPhase: 'reserved' | 'running' | 'awaiting-approval'
  reason: string
}

export const isFlagEffect = (effect: TopicStreamEffect): effect is TopicStreamFlagEffect =>
  effect.type === 'set-ring-eviction'

export type TopicStreamEffect = TopicStreamFlagEffect | TopicStreamWorkEffect

export type TopicCommandRejection =
  | 'stale-cycle'
  | 'unknown-attempt'
  | 'duplicate-attempt'
  | 'invalid-attempt-state'
  | 'busy'
  | 'invalid-approval'
  | 'invalid-continuation'
  | 'evicted'

export interface TopicReducerResult {
  readonly state: TopicStreamState
  readonly events: readonly TopicStreamEvent[]
  readonly effects: readonly TopicStreamEffect[]
  readonly changed: boolean
  readonly rejection?: TopicCommandRejection
}

export function createTopicStreamState(topicId: string, cycleId: number): TopicStreamState {
  return {
    topicId,
    cycleId,
    revision: 0,
    lifecycle: 'active',
    attempts: new Map(),
    continuationLeases: new Map(),
    pendingChatSteers: []
  }
}

/** True when the attempt just reached a terminal whose published outcome is an error. */
const landsOnError = (state: AttemptState): boolean =>
  (state.phase === 'finalizing' || state.phase === 'settled') && state.outcome.kind === 'error'

const isTerminalPhase = (state: AttemptState): boolean => state.phase === 'settled' || state.phase === 'abandoned'

const stateHasUnsettledAttempts = (state: TopicStreamState): boolean =>
  [...state.attempts.values()].some((attempt) => !isTerminalPhase(attempt.state))

const stateHasRunningAttempts = (state: TopicStreamState): boolean =>
  [...state.attempts.values()].some((attempt) => attempt.state.phase === 'running')

const stateHasOpenLease = (state: TopicStreamState): boolean =>
  [...state.continuationLeases.values()].some((lease) => lease.state === 'open')

const stateHasPendingApprovals = (state: TopicStreamState): boolean =>
  [...state.attempts.values()].some((attempt) => attempt.pendingApprovalToolCallIds.size > 0)

const unchanged = (state: TopicStreamState, rejection?: TopicCommandRejection): TopicReducerResult => ({
  state,
  events: [],
  effects: [],
  changed: false,
  rejection
})

const withAttempts = (
  state: TopicStreamState,
  attempts: ReadonlyMap<AttemptId, TopicAttemptState>
): TopicStreamState => ({ ...state, attempts, revision: state.revision + 1 })

const replaceAttempt = (state: TopicStreamState, attempt: TopicAttemptState): TopicStreamState => {
  const attempts = new Map(state.attempts)
  attempts.set(attempt.id, attempt)
  return withAttempts(state, attempts)
}

const replaceLease = (state: TopicStreamState, lease: TopicContinuationLease): TopicStreamState => {
  const continuationLeases = new Map(state.continuationLeases)
  continuationLeases.set(lease.id, lease)
  return { ...state, continuationLeases, revision: state.revision + 1 }
}

/** Pure topic-control reducer. Never performs IO and never reads a resource. */
export function reduceTopicStream(state: TopicStreamState, command: TopicStreamCommand): TopicReducerResult {
  if (state.lifecycle === 'evicted' && command.type !== 'activate') return unchanged(state, 'evicted')

  switch (command.type) {
    case 'reserve-dispatch': {
      if (command.attemptIds.length === 0) return unchanged(state)
      if (command.attemptIds.some((id) => state.attempts.has(id))) return unchanged(state, 'duplicate-attempt')
      const continuation =
        command.reservation.kind === 'continuation'
          ? state.continuationLeases.get(command.reservation.leaseId)
          : undefined
      const freshOwnershipLeaseId =
        command.reservation.kind === 'fresh' ? command.reservation.ownershipLeaseId : undefined
      if (
        command.reservation.kind === 'fresh' &&
        (stateHasUnsettledAttempts(state) ||
          [...state.continuationLeases.values()].some(
            (lease) => lease.state === 'open' && lease.id !== freshOwnershipLeaseId
          ) ||
          stateHasPendingApprovals(state))
      ) {
        return unchanged(state, 'busy')
      }
      if (command.reservation.kind === 'fresh' && command.reservation.ownershipLeaseId) {
        const ownership = state.continuationLeases.get(command.reservation.ownershipLeaseId)
        if (ownership?.state !== 'open' || ownership.kind !== 'runtime-ownership') {
          return unchanged(state, 'invalid-continuation')
        }
      }
      if (command.reservation.kind === 'live-change' && !stateHasRunningAttempts(state)) {
        return unchanged(state, 'busy')
      }
      const approvalAttempt =
        command.reservation.kind === 'approval-resume' ? state.attempts.get(command.reservation.attemptId) : undefined
      if (command.reservation.kind === 'approval-resume') {
        const approvalAttemptId = command.reservation.attemptId
        const otherUnsettled = [...state.attempts.values()].some(
          (attempt) => attempt.id !== approvalAttemptId && !isTerminalPhase(attempt.state)
        )
        if (
          approvalAttempt?.state.phase !== 'awaiting-approval' ||
          approvalAttempt.pendingApprovalToolCallIds.size === 0 ||
          otherUnsettled ||
          stateHasOpenLease(state)
        ) {
          return unchanged(state, 'invalid-approval')
        }
      }
      if (command.reservation.kind === 'continuation' && continuation?.state !== 'open') {
        return unchanged(state, 'invalid-continuation')
      }
      if (command.reservation.kind === 'continuation' && stateHasUnsettledAttempts(state)) {
        return unchanged(state, 'busy')
      }
      if (command.reservation.kind === 'continuation' && command.reservation.chatSteerId) {
        const head = state.pendingChatSteers[0]
        if (head?.id !== command.reservation.chatSteerId || head.leaseId !== command.reservation.leaseId) {
          return unchanged(state, 'invalid-continuation')
        }
      }
      const attempts = new Map(state.attempts)
      const effects: TopicStreamEffect[] = []
      const events: TopicStreamEvent[] = []
      if (command.reservation.kind === 'approval-resume' && approvalAttempt) {
        const resumed = transition(approvalAttempt.state, { type: 'approval-resumed' })
        if (!resumed.ok) return unchanged(state, 'invalid-approval')
        attempts.set(approvalAttempt.id, {
          ...approvalAttempt,
          state: resumed.state,
          pendingApprovalToolCallIds: new Set()
        })
        events.push({ type: 'attempt-changed', attemptId: approvalAttempt.id, state: resumed.state })
        events.push({ type: 'approval-changed', attemptId: approvalAttempt.id, pending: false })
        effects.push({ type: 'set-ring-eviction', attemptId: approvalAttempt.id, paused: false })
      }
      for (const id of command.attemptIds) {
        attempts.set(id, { id, state: { phase: 'reserved' }, pendingApprovalToolCallIds: new Set() })
      }
      let next: TopicStreamState = {
        ...state,
        attempts,
        pendingChatSteers:
          command.reservation.kind === 'continuation' && command.reservation.chatSteerId
            ? state.pendingChatSteers.slice(1)
            : state.pendingChatSteers,
        revision: state.revision + 1
      }
      events.push(...command.attemptIds.map((attemptId): TopicStreamEvent => ({ type: 'attempt-reserved', attemptId })))
      if (command.reservation.kind === 'continuation' && continuation?.state === 'open') {
        const consumed: TopicContinuationLease = {
          ...continuation,
          state: 'consumed',
          attemptId: command.attemptIds[0]
        }
        next = replaceLease(next, consumed)
        events.push({ type: 'continuation-changed', lease: consumed })
      }
      if (command.reservation.kind === 'continuation' && command.reservation.ownershipLeaseId) {
        const ownership = next.continuationLeases.get(command.reservation.ownershipLeaseId)
        if (ownership?.state !== 'open' || ownership.kind !== 'runtime-ownership') {
          return unchanged(state, 'invalid-continuation')
        }
        const consumed: TopicContinuationLease = {
          ...ownership,
          state: 'consumed',
          attemptId: command.attemptIds[0]
        }
        next = replaceLease(next, consumed)
        events.push({ type: 'continuation-changed', lease: consumed })
      }
      if (command.reservation.kind === 'fresh' && command.reservation.ownershipLeaseId) {
        const ownership = next.continuationLeases.get(command.reservation.ownershipLeaseId)
        if (ownership?.state !== 'open' || ownership.kind !== 'runtime-ownership') {
          return unchanged(state, 'invalid-continuation')
        }
        const consumed: TopicContinuationLease = {
          ...ownership,
          state: 'consumed',
          attemptId: command.attemptIds[0]
        }
        next = replaceLease(next, consumed)
        events.push({ type: 'continuation-changed', lease: consumed })
      }
      return {
        state: next,
        events,
        effects,
        changed: true
      }
    }

    case 'reserve-attempt': {
      if (state.attempts.has(command.attemptId)) return unchanged(state, 'duplicate-attempt')
      const attempt: TopicAttemptState = {
        id: command.attemptId,
        state: { phase: 'reserved' },
        pendingApprovalToolCallIds: new Set()
      }
      return {
        state: replaceAttempt(state, attempt),
        events: [{ type: 'attempt-reserved', attemptId: attempt.id }],
        effects: [],
        changed: true
      }
    }

    case 'forget-attempt': {
      if (!state.attempts.has(command.attemptId)) return unchanged(state)
      const attempts = new Map(state.attempts)
      attempts.delete(command.attemptId)
      return { state: withAttempts(state, attempts), events: [], effects: [], changed: true }
    }

    case 'attempt-event': {
      const attempt = state.attempts.get(command.attemptId)
      if (!attempt) return unchanged(state, 'unknown-attempt')
      const result = transition(attempt.state, command.event)
      if (!result.ok) return unchanged(state, 'invalid-attempt-state')
      if (result.state === attempt.state) return unchanged(state)
      let next = replaceAttempt(state, { ...attempt, state: result.state })
      const events: TopicStreamEvent[] = [{ type: 'attempt-changed', attemptId: attempt.id, state: result.state }]
      // An error terminal voids every lease whose promised work depended on this attempt
      // succeeding, in the same transition — no later observer can see it still open.
      if (landsOnError(result.state)) {
        for (const lease of next.continuationLeases.values()) {
          if (lease.state !== 'open' || !lease.voidOnAttemptError) continue
          const released: TopicContinuationLease = { ...lease, state: 'released', reason: 'source-error' }
          next = replaceLease(next, released)
          events.push({ type: 'continuation-changed', lease: released })
        }
      }
      return { state: next, events, effects: [], changed: true }
    }

    case 'approval-changed': {
      const attempt = state.attempts.get(command.attemptId)
      if (!attempt) return unchanged(state, 'unknown-attempt')
      const had = attempt.pendingApprovalToolCallIds.size > 0
      const next = new Set(attempt.pendingApprovalToolCallIds)
      if (command.pending) next.add(command.toolCallId)
      else next.delete(command.toolCallId)
      if (next.size === attempt.pendingApprovalToolCallIds.size) return unchanged(state)
      const pending = next.size > 0
      return {
        state: replaceAttempt(state, { ...attempt, pendingApprovalToolCallIds: next }),
        events: had === pending ? [] : [{ type: 'approval-changed', attemptId: attempt.id, pending }],
        // Eviction must pause while any approval is open (#17922); pushed, never read back.
        effects: had === pending ? [] : [{ type: 'set-ring-eviction', attemptId: attempt.id, paused: pending }],
        changed: true
      }
    }

    case 'approvals-cleared': {
      const attempt = state.attempts.get(command.attemptId)
      if (!attempt?.pendingApprovalToolCallIds.size) return unchanged(state)
      return {
        state: replaceAttempt(state, { ...attempt, pendingApprovalToolCallIds: new Set() }),
        events: [{ type: 'approval-changed', attemptId: attempt.id, pending: false }],
        effects: [{ type: 'set-ring-eviction', attemptId: attempt.id, paused: false }],
        changed: true
      }
    }

    case 'enqueue-chat-steer': {
      if (state.pendingChatSteers.some((steer) => steer.id === command.steer.id)) return unchanged(state)
      if (state.continuationLeases.has(command.steer.leaseId)) return unchanged(state, 'invalid-continuation')
      const lease: TopicContinuationLease = {
        id: command.steer.leaseId,
        topicId: state.topicId,
        cycleId: state.cycleId,
        diagnosticOwner: 'chat-steer',
        kind: 'continuation',
        voidOnAttemptError: false,
        state: 'open'
      }
      const continuationLeases = new Map(state.continuationLeases)
      continuationLeases.set(lease.id, lease)
      return {
        state: {
          ...state,
          continuationLeases,
          pendingChatSteers: [...state.pendingChatSteers, command.steer],
          revision: state.revision + 1
        },
        events: [{ type: 'continuation-changed', lease }],
        effects: [],
        changed: true
      }
    }

    case 'drop-chat-steers': {
      if (state.pendingChatSteers.length === 0) return unchanged(state)
      let next = state
      const events: TopicStreamEvent[] = []
      for (const steer of state.pendingChatSteers) {
        const lease = next.continuationLeases.get(steer.leaseId)
        if (lease?.state !== 'open') continue
        const released: TopicContinuationLease = { ...lease, state: 'released', reason: command.reason }
        next = replaceLease(next, released)
        events.push({ type: 'continuation-changed', lease: released })
      }
      return {
        state: { ...next, pendingChatSteers: [], revision: next.revision + 1 },
        events,
        effects: [],
        changed: true
      }
    }

    case 'stop-topic': {
      const attempts = new Map(state.attempts)
      const continuationLeases = new Map(state.continuationLeases)
      const events: TopicStreamEvent[] = []
      const effects: TopicStreamEffect[] = []
      let changed = false

      for (const attempt of state.attempts.values()) {
        const phase = attempt.state.phase
        if (phase !== 'reserved' && phase !== 'running' && phase !== 'awaiting-approval') continue
        const stopped = transition(attempt.state, { type: 'abort', reason: command.reason })
        if (!stopped.ok) continue
        attempts.set(attempt.id, {
          ...attempt,
          state: stopped.state,
          pendingApprovalToolCallIds: new Set()
        })
        events.push({ type: 'attempt-changed', attemptId: attempt.id, state: stopped.state })
        if (attempt.pendingApprovalToolCallIds.size > 0) {
          events.push({ type: 'approval-changed', attemptId: attempt.id, pending: false })
          effects.push({ type: 'set-ring-eviction', attemptId: attempt.id, paused: false })
        }
        effects.push({ type: 'stop-attempt', attemptId: attempt.id, priorPhase: phase, reason: command.reason })
        changed = true
      }

      for (const lease of state.continuationLeases.values()) {
        if (lease.state !== 'open' || lease.kind === 'runtime-ownership') continue
        const released: TopicContinuationLease = { ...lease, state: 'released', reason: 'stop' }
        continuationLeases.set(lease.id, released)
        events.push({ type: 'continuation-changed', lease: released })
        changed = true
      }

      if (state.pendingChatSteers.length > 0) changed = true
      if (!changed) return unchanged(state)
      return {
        state: {
          ...state,
          attempts,
          continuationLeases,
          pendingChatSteers: [],
          revision: state.revision + 1
        },
        events,
        effects,
        changed: true
      }
    }

    case 'dispatch-preparation-failed': {
      const attempts = new Map(state.attempts)
      const events: TopicStreamEvent[] = []
      let changed = false
      for (const attemptId of command.attemptIds) {
        const attempt = attempts.get(attemptId)
        if (!attempt || attempt.state.phase !== 'reserved') continue
        const failed = transition(attempt.state, {
          type: 'reservation-failed',
          error: command.error,
          durableErrorWritten: false
        })
        if (!failed.ok) continue
        attempts.set(attemptId, { ...attempt, state: failed.state })
        events.push({ type: 'attempt-changed', attemptId, state: failed.state })
        changed = true
      }
      if (!changed) return unchanged(state)
      return {
        state: { ...state, attempts, revision: state.revision + 1 },
        events,
        effects: [],
        changed: true
      }
    }

    case 'continuation-opened': {
      if (state.continuationLeases.has(command.leaseId)) return unchanged(state, 'invalid-continuation')
      const lease: TopicContinuationLease = {
        id: command.leaseId,
        topicId: state.topicId,
        cycleId: state.cycleId,
        diagnosticOwner: command.diagnosticOwner,
        kind: command.kind ?? 'continuation',
        voidOnAttemptError: command.voidOnAttemptError === true,
        state: 'open'
      }
      return {
        state: replaceLease(state, lease),
        events: [{ type: 'continuation-changed', lease }],
        effects: [],
        changed: true
      }
    }

    case 'continuation-updated': {
      const existing = state.continuationLeases.get(command.leaseId)
      if (!existing || existing.state !== 'open') return unchanged(state, 'invalid-continuation')
      if (existing.voidOnAttemptError === command.voidOnAttemptError) return unchanged(state)
      const lease: TopicContinuationLease = { ...existing, voidOnAttemptError: command.voidOnAttemptError }
      return {
        state: replaceLease(state, lease),
        events: [{ type: 'continuation-changed', lease }],
        effects: [],
        changed: true
      }
    }

    case 'continuation-consumed':
    case 'continuation-released': {
      const existing = state.continuationLeases.get(command.leaseId)
      // L2: open → consumed | released, both idempotent terminals and mutually exclusive.
      if (!existing) return unchanged(state, 'invalid-continuation')
      if (existing.state !== 'open') return unchanged(state)
      const lease: TopicContinuationLease =
        command.type === 'continuation-consumed'
          ? { ...existing, state: 'consumed', attemptId: command.attemptId }
          : { ...existing, state: 'released', reason: command.reason }
      return {
        state: replaceLease(state, lease),
        events: [{ type: 'continuation-changed', lease }],
        effects: [],
        changed: true
      }
    }

    case 'activate':
      if (state.lifecycle === 'active') return unchanged(state)
      return {
        state: { ...state, lifecycle: 'active', revision: state.revision + 1 },
        events: [{ type: 'lifecycle-changed', lifecycle: 'active' }],
        effects: [],
        changed: true
      }

    case 'begin-grace':
      if (state.lifecycle === 'grace') return unchanged(state)
      return {
        state: { ...state, lifecycle: 'grace', revision: state.revision + 1 },
        events: [{ type: 'lifecycle-changed', lifecycle: 'grace' }],
        effects: [],
        changed: true
      }

    case 'evict':
      return {
        state: {
          ...state,
          lifecycle: 'evicted',
          attempts: new Map(),
          continuationLeases: new Map(),
          pendingChatSteers: [],
          revision: state.revision + 1
        },
        events: [{ type: 'lifecycle-changed', lifecycle: 'evicted' }],
        effects: [],
        changed: true
      }

    case 'touch':
      return { state: { ...state, revision: state.revision + 1 }, events: [], effects: [], changed: true }
  }
}

// ── Pure selectors ────────────────────────────────────────────────────

export const hasOpenLease = (state: TopicStreamState): boolean => stateHasOpenLease(state)

export const hasUnsettledAttempts = (state: TopicStreamState): boolean => stateHasUnsettledAttempts(state)

export const hasPersistenceBlockedAttempts = (state: TopicStreamState): boolean =>
  [...state.attempts.values()].some((attempt) => attempt.state.phase === 'persistence-blocked')

export const hasPendingApprovals = (state: TopicStreamState): boolean => stateHasPendingApprovals(state)

/** T6: quiescence is derived, never toggled by a side flag. */
export const areAttemptsDurablySettled = (state: TopicStreamState): boolean => {
  if (state.attempts.size === 0) return false
  return [...state.attempts.values()].every(
    (attempt) => isTerminalPhase(attempt.state) && attempt.pendingApprovalToolCallIds.size === 0
  )
}

export const isQuiescent = (state: TopicStreamState): boolean =>
  areAttemptsDurablySettled(state) && !hasOpenLease(state)

export const attemptWatermark = (state: TopicStreamState): number => {
  let watermark = 0
  for (const attempt of state.attempts.values()) watermark = Math.max(watermark, attempt.id)
  return watermark
}

/** A released lease folds into topic outcome; a consumed one is carried by its attempt instead. */
const releasedLeaseOutcome = (reason: ContinuationReleaseReason): 'error' | 'aborted' | undefined => {
  switch (reason) {
    case 'launch-failed':
    case 'source-error':
      return 'error'
    case 'stop':
    case 'queue-cleared':
      return 'aborted'
    case 'handoff-rejected':
      return undefined
  }
}

const leaseOutcomes = (state: TopicStreamState): Array<'error' | 'aborted'> =>
  [...state.continuationLeases.values()].flatMap((lease) => {
    if (lease.state !== 'released') return []
    const outcome = releasedLeaseOutcome(lease.reason)
    return outcome ? [outcome] : []
  })

export function topicStatus(state: TopicStreamState): TopicStreamStatus {
  const status = reduceTopicStatus(
    [...state.attempts.values()].map((attempt) => ({
      state: attempt.state,
      pendingApprovals: attempt.pendingApprovalToolCallIds
    }))
  )
  if (status === 'pending' || status === 'streaming' || status === 'awaiting-approval') return status
  if (hasOpenLease(state)) return 'streaming'
  const outcomes = leaseOutcomes(state)
  // T7: error dominates aborted dominates done, independent of arrival order.
  if (outcomes.includes('error')) return 'error'
  if (status === 'done' && outcomes.includes('aborted')) return 'aborted'
  return status
}

export function runtimeOutcome(
  state: TopicStreamState
): 'done' | 'error' | 'aborted' | 'awaiting-approval' | undefined {
  const attempts = [...state.attempts.values()]
  if (
    attempts.length === 0 ||
    attempts.some((attempt) => attempt.state.phase === 'reserved' || attempt.state.phase === 'running')
  ) {
    return undefined
  }
  if (attempts.some((attempt) => attempt.state.phase === 'awaiting-approval')) return 'awaiting-approval'
  if (attempts.some((attempt) => attempt.pendingApprovalToolCallIds.size > 0)) return 'awaiting-approval'

  const outcomes: AttemptOutcome[] = attempts.map((attempt) => {
    if (
      attempt.state.phase === 'reserved' ||
      attempt.state.phase === 'running' ||
      attempt.state.phase === 'awaiting-approval'
    ) {
      throw new Error(`Attempt ${attempt.id} has no runtime outcome`)
    }
    return attempt.state.outcome
  })
  const released = leaseOutcomes(state)
  if (outcomes.some((outcome) => outcome.kind === 'error') || released.includes('error')) return 'error'
  if (outcomes.every((outcome) => outcome.kind === 'aborted')) return 'aborted'
  if (released.includes('aborted')) return 'aborted'
  return 'done'
}

export type { AttemptEvent, AttemptOutcome, AttemptState, SerializedError }
