/**
 * React binding for {@link executionStreamOverlayService}, which owns the
 * per-execution streaming overlay (readers, snapshots, rAF batching) keyed by
 * `topicId`. This hook only acquires/releases a refcounted view, feeds the
 * service the consumer-visible execution set + DB seed rows, and reads the
 * retained view via `useSyncExternalStore` — so unmounting (route/tab/conversation
 * switch) no longer tears the stream down, and remounting restores the live
 * overlay synchronously. Reader/seed semantics live in the service.
 */
import { executionStreamOverlayService } from '@renderer/services/aiTransport'
import type { ActiveExecution, ActiveNodeDecision } from '@shared/ai/transport'
import type { CherryMessagePart, CherryUIMessage } from '@shared/data/types/message'
import { useCallback, useEffect, useRef, useSyncExternalStore } from 'react'

export type { ExecutionFinishEvent } from '@renderer/services/aiTransport'
import type {
  ExecutionFinishEvent,
  ExecutionOverlayActiveNodeOverride,
  ExecutionOverlayAttempt
} from '@renderer/services/aiTransport'

export interface UseExecutionOverlayOptions {
  onFinish?: (executionId: string, event: ExecutionFinishEvent) => void
  /** Persistent projections refresh committed rows at TopicQuiesced, then retire final overlays. */
  refreshOnQuiesced?: () => Promise<unknown>
}

export interface ExecutionOverlayApi {
  /** messageId -> latest streamed parts. messageId = anchorMessageId, or the
   *  start-chunk id when the execution has no pre-allocated row (temp topic). */
  overlay: Record<string, CherryMessagePart[]>
  /** Latest assistant snapshot per execution, in insertion order. */
  liveAssistants: CherryUIMessage[]
  /** Attempt records whose message stays stable while phase changes active → settled. */
  attempts: ExecutionOverlayAttempt[]
  optimisticMessages: CherryUIMessage[]
  projectedExecutions: ActiveExecution[]
  activeNodeOverride: ExecutionOverlayActiveNodeOverride | null
  refreshError: Error | null
  seedReservations: (
    messages: readonly CherryUIMessage[],
    executions: readonly ActiveExecution[],
    activeNodeDecision: ActiveNodeDecision | undefined,
    previousActiveNodeId: string | null
  ) => void
  /** Drop one overlay/snapshot entry by its message id (post-persist handoff). */
  disposeOverlay: (messageId: string) => void
  /** Drop settled overlay/snapshot entries (terminal handoff); live readers survive. */
  reset: () => void
  /** Destructively drop every overlay/snapshot entry (quick-assistant clear()). */
  clear: () => void
}

interface TopicOverlayBinding {
  readonly topicId: string
  readonly consumer: object
  uiMessages: CherryUIMessage[]
  onFinish: UseExecutionOverlayOptions['onFinish']
  refreshOnQuiesced: UseExecutionOverlayOptions['refreshOnQuiesced']
  readonly getSeedMessages: () => CherryUIMessage[]
}

function createTopicOverlayBinding(topicId: string): TopicOverlayBinding {
  const binding = {
    topicId,
    consumer: {},
    uiMessages: [],
    onFinish: undefined,
    refreshOnQuiesced: undefined,
    getSeedMessages: () => binding.uiMessages
  } satisfies TopicOverlayBinding
  return binding
}

export function useExecutionOverlay(
  topicId: string,
  activeExecutions: readonly ActiveExecution[],
  uiMessages: CherryUIMessage[],
  options: UseExecutionOverlayOptions = {}
): ExecutionOverlayApi {
  const bindingRef = useRef<TopicOverlayBinding>(undefined)
  if (!bindingRef.current || bindingRef.current.topicId !== topicId) {
    bindingRef.current = createTopicOverlayBinding(topicId)
  }
  const binding = bindingRef.current
  binding.uiMessages = uiMessages
  binding.onFinish = options.onFinish
  binding.refreshOnQuiesced = options.refreshOnQuiesced

  // Declared before the sync effect so acquisition (entry creation) always
  // precedes reader convergence for a new topicId.
  useEffect(() => {
    executionStreamOverlayService.acquire(binding.topicId)
    const offFinish = executionStreamOverlayService.onFinish(binding.topicId, (executionId, event) =>
      binding.onFinish?.(executionId, event)
    )
    // The service owns the quiesce → refresh → retire handoff (and its retry);
    // this only lends it the consumer's DB refetch while mounted.
    const offRefresh = binding.refreshOnQuiesced
      ? executionStreamOverlayService.registerRefreshPort(
          binding.topicId,
          () => binding.refreshOnQuiesced?.() ?? Promise.resolve()
        )
      : undefined
    return () => {
      offFinish()
      offRefresh?.()
      executionStreamOverlayService.release(binding.topicId, binding.consumer)
    }
  }, [binding])

  // No cleanup: departure must not cancel readers (release() handles removal).
  useEffect(() => {
    executionStreamOverlayService.syncExecutions(
      binding.topicId,
      binding.consumer,
      activeExecutions,
      binding.getSeedMessages
    )
  }, [activeExecutions, binding])

  const subscribe = useCallback(
    (listener: () => void) => executionStreamOverlayService.subscribe(binding.topicId, listener),
    [binding]
  )
  const view = useSyncExternalStore(
    subscribe,
    useCallback(() => executionStreamOverlayService.getView(binding.topicId), [binding])
  )

  const api = useRef<{ binding: TopicOverlayBinding; value: ExecutionOverlayApi }>(undefined)
  if (!api.current || api.current.binding !== binding) {
    const value: ExecutionOverlayApi = {
      overlay: view.overlay,
      liveAssistants: view.liveAssistants,
      attempts: view.attempts,
      optimisticMessages: view.optimisticMessages,
      projectedExecutions: view.projectedExecutions,
      activeNodeOverride: view.activeNodeOverride,
      refreshError: view.refreshError,
      seedReservations: (messages, executions, activeNodeDecision, previousActiveNodeId) =>
        executionStreamOverlayService.seedReservations(
          binding.topicId,
          messages,
          executions,
          activeNodeDecision,
          previousActiveNodeId,
          binding.getSeedMessages
        ),
      disposeOverlay: (messageId: string) => executionStreamOverlayService.disposeOverlay(binding.topicId, messageId),
      reset: () => executionStreamOverlayService.reset(binding.topicId),
      clear: () => executionStreamOverlayService.clear(binding.topicId)
    }
    api.current = {
      binding,
      value
    }
  }
  api.current.value.overlay = view.overlay
  api.current.value.liveAssistants = view.liveAssistants
  api.current.value.attempts = view.attempts
  api.current.value.optimisticMessages = view.optimisticMessages
  api.current.value.projectedExecutions = view.projectedExecutions
  api.current.value.activeNodeOverride = view.activeNodeOverride
  api.current.value.refreshError = view.refreshError
  return api.current.value
}
