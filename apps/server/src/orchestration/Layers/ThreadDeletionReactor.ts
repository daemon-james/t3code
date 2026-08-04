import type { OrchestrationEvent } from "@t3tools/contracts";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import { ProviderService } from "../../provider/Services/ProviderService.ts";
import * as TerminalManager from "../../terminal/Manager.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import {
  ThreadDeletionReactor,
  type ThreadDeletionReactorShape,
} from "../Services/ThreadDeletionReactor.ts";

type ThreadDeletedEvent = Extract<OrchestrationEvent, { type: "thread.deleted" }>;
type ThreadArchivedEvent = Extract<OrchestrationEvent, { type: "thread.archived" }>;
type ThreadLifecycleEvent = ThreadDeletedEvent | ThreadArchivedEvent;
type ReactorThreadId = ThreadDeletedEvent["payload"]["threadId"];

export const logCleanupCauseUnlessInterrupted = <R, E>({
  effect,
  message,
  threadId,
}: {
  readonly effect: Effect.Effect<void, E, R>;
  readonly message: string;
  readonly threadId: ReactorThreadId;
}): Effect.Effect<void, E, R> =>
  effect.pipe(
    Effect.catchCause((cause) => {
      if (Cause.hasInterruptsOnly(cause)) {
        return Effect.failCause(cause);
      }
      return Effect.logDebug(message, {
        threadId,
        cause: Cause.pretty(cause),
      });
    }),
  );

const make = Effect.gen(function* () {
  const orchestrationEngine = yield* OrchestrationEngineService;
  const providerService = yield* ProviderService;
  const terminalManager = yield* TerminalManager.TerminalManager;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;

  const stopProviderSession = (threadId: ReactorThreadId, message: string) =>
    logCleanupCauseUnlessInterrupted({
      effect: providerService.stopSession({ threadId }),
      message,
      threadId,
    });

  const closeThreadTerminals = (threadId: ReactorThreadId) =>
    logCleanupCauseUnlessInterrupted({
      effect: terminalManager.close({ threadId, deleteHistory: true }),
      message: "thread deletion cleanup skipped terminal close",
      threadId,
    });

  const processThreadDeleted = Effect.fn("processThreadDeleted")(function* (
    event: ThreadDeletedEvent,
  ) {
    const { threadId } = event.payload;
    yield* stopProviderSession(threadId, "thread deletion cleanup skipped provider session stop");
    yield* closeThreadTerminals(threadId);
  });

  /**
   * Archiving is an explicit "I am done with this thread" signal, so the provider
   * session is stopped immediately rather than left for the idle reaper.
   *
   * `ProviderSessionReaper` would eventually collect it, but only after
   * `DEFAULT_INACTIVITY_THRESHOLD_MS` (30 min) plus up to one sweep interval
   * (5 min). Until then the session keeps its provider process alive along with
   * every MCP server configured for it — on a host with a large MCP config that
   * is hundreds of MB per archived thread, and archiving several in a row stacks
   * them. Waiting is the right default for *idle* threads, where the server
   * cannot tell "stepped away" from "finished"; archiving removes that ambiguity.
   *
   * Terminals are intentionally left alone: archiving is reversible
   * (`thread.unarchived`), so terminal history must survive it. Sessions restart
   * on demand, which is what already makes the idle reaper safe.
   */
  const processThreadArchived = Effect.fn("processThreadArchived")(function* (
    event: ThreadArchivedEvent,
  ) {
    const { threadId } = event.payload;

    // Archiving is permitted mid-turn, so never interrupt in-flight work; the
    // idle reaper applies the same guard and will collect it once the turn ends.
    const thread = yield* projectionSnapshotQuery.getThreadShellById(threadId).pipe(
      Effect.map(Option.getOrUndefined),
      Effect.orElseSucceed(() => undefined),
    );
    if (thread?.session?.activeTurnId != null) {
      yield* Effect.logDebug("thread archive cleanup skipped active turn", {
        threadId,
        activeTurnId: thread.session.activeTurnId,
      });
      return;
    }

    yield* stopProviderSession(threadId, "thread archive cleanup skipped provider session stop");
  });

  const processThreadEvent = (event: ThreadLifecycleEvent) =>
    event.type === "thread.deleted" ? processThreadDeleted(event) : processThreadArchived(event);

  const processThreadEventSafely = (event: ThreadLifecycleEvent) =>
    processThreadEvent(event).pipe(
      Effect.catchCause((cause) => {
        if (Cause.hasInterruptsOnly(cause)) {
          return Effect.failCause(cause);
        }
        return Effect.logWarning("thread lifecycle reactor failed to process event", {
          eventType: event.type,
          threadId: event.payload.threadId,
          cause: Cause.pretty(cause),
        });
      }),
    );

  const worker = yield* makeDrainableWorker(processThreadEventSafely);

  const start: ThreadDeletionReactorShape["start"] = Effect.fn("start")(function* () {
    yield* Effect.forkScoped(
      Stream.runForEach(orchestrationEngine.streamDomainEvents, (event) => {
        if (event.type !== "thread.deleted" && event.type !== "thread.archived") {
          return Effect.void;
        }
        return worker.enqueue(event);
      }),
    );
  });

  return {
    start,
    drain: worker.drain,
  } satisfies ThreadDeletionReactorShape;
});

export const ThreadDeletionReactorLive = Layer.effect(ThreadDeletionReactor, make);
