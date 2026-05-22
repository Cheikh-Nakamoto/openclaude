import { sleep } from './sleep.js'
import { logForDebugging } from './debug.js'

// ---------------------------------------------------------------------------
// Configuration helpers
// ---------------------------------------------------------------------------

/**
 * Lit la limite maximale de requêtes par minute (RPM) depuis les variables
 * d'environnement. `OPENCLAUDE_MAX_RPM` a la priorité sur `CLAUDE_CODE_MAX_RPM`.
 * Retourne 0 si aucune variable n'est définie ou si la valeur est invalide,
 * ce qui désactive entièrement le rate limiting.
 */
export function getMaxRpm(): number {
  const openVal = process.env.OPENCLAUDE_MAX_RPM
  if (openVal) {
    const parsed = parseInt(openVal, 10)
    if (!isNaN(parsed) && parsed > 0) return parsed
  }

  const claudeVal = process.env.CLAUDE_CODE_MAX_RPM
  if (claudeVal) {
    const parsed = parseInt(claudeVal, 10)
    if (!isNaN(parsed) && parsed > 0) return parsed
  }

  return 0
}

/**
 * Retourne la taille du burst initial autorisé.
 * Le bucket est initialisé avec ce nombre de tokens au premier appel,
 * ce qui permet aux premières requêtes de passer sans attendre le premier
 * cycle de refill. Minimum : 1 (au moins 1 token au démarrage).
 * Configurable via OPENCLAUDE_BURST_SIZE (défaut : 2).
 */
function getBurstSize(): number {
  const envVal = process.env.OPENCLAUDE_BURST_SIZE
  if (envVal) {
    const parsed = parseInt(envVal, 10)
    if (!isNaN(parsed) && parsed >= 1) return parsed
  }
  return 2 // petit burst autorisé au démarrage
}

/**
 * Retourne la capacité maximale de la file d'attente (FIFO queue).
 * Si plus de `queueMax` requêtes sont en attente, les nouvelles entrantes
 * sont rejetées immédiatement avec une `QueueFullError` pour éviter une
 * accumulation mémoire infinie sous forte charge.
 * Configurable via OPENCLAUDE_QUEUE_MAX (défaut : 50).
 */
function getQueueMax(): number {
  const envVal = process.env.OPENCLAUDE_QUEUE_MAX
  if (envVal) {
    const parsed = parseInt(envVal, 10)
    if (!isNaN(parsed) && parsed >= 0) return parsed
  }
  return 50
}

// ---------------------------------------------------------------------------
// Custom errors
// ---------------------------------------------------------------------------

/**
 * Erreur levée quand la file d'attente est saturée.
 * Indique que l'API est surchargée ou que la limite RPM est trop basse
 * pour le volume de requêtes généré par les subagents.
 */
export class QueueFullError extends Error {
  constructor(queueMax: number) {
    super(
      `Rate limiter request queue is full (max ${queueMax}). ` +
        'The API endpoint is being throttled. Please try again later, ' +
        'or increase OPENCLAUDE_QUEUE_MAX.',
    )
    this.name = 'QueueFullError'
  }
}

// ---------------------------------------------------------------------------
// Token Bucket state (module-level singletons)
// ---------------------------------------------------------------------------

/**
 * Représente une requête en attente dans la FIFO queue.
 * `resolve` est appelé quand un token est acquis et que la requête peut partir.
 * `reject` est appelé si la requête est annulée (AbortSignal) ou si le
 * rate limiter est réinitialisé (tests).
 * `signal` est un AbortSignal optionnel permettant d'interrompre l'attente
 * et de retirer la requête de la queue si l'opération parente est annulée.
 */
interface WaitEntry {
  resolve: () => void
  reject: (err: Error) => void
  signal?: AbortSignal
}

// Nombre de tokens disponibles dans le bucket à l'instant t.
let tokens = 0

// Timestamp (ms) du dernier refill calculé. Utilisé pour déterminer combien
// de tokens accumulés ont été générés depuis le dernier appel à refillTokens().
let lastRefillTime = 0

// Référence au timer de refill en cours. `null` = pas de timer actif.
// On n'en schedule qu'un seul à la fois pour éviter les doubles dispatches.
let refillTimer: ReturnType<typeof setTimeout> | null = null

// Timestamp (ms) jusqu'auquel le bucket est en pause (suite à un 429).
// Tant que Date.now() < pausedUntil, aucun token n'est distribué.
let pausedUntil = 0

// File d'attente FIFO des requêtes en attente d'un token.
// Chaque entrée est une promesse suspendue dans checkRateLimit().
const waitQueue: WaitEntry[] = []

// ---------------------------------------------------------------------------
// Bucket internals
// ---------------------------------------------------------------------------

/**
 * Calcule et ajoute les tokens accumulés depuis le dernier refill.
 *
 * Fonctionnement :
 * - À chaque appel, on calcule le temps écoulé depuis `lastRefillTime`.
 * - On en déduit combien de cycles de refill se sont écoulés
 *   (`intervalMs = 60_000 / maxRpm`).
 * - On ajoute ce nombre de tokens, plafonné à `burstSize`.
 * - On avance `lastRefillTime` d'exactement `newTokens * intervalMs` ms
 *   pour éviter la dérive d'horloge (on ne perd pas les fractions non consommées).
 *
 * Au premier appel (lastRefillTime === 0), on initialise directement le bucket
 * avec `burstSize` tokens pour que les premières requêtes ne soient pas bloquées.
 */
function refillTokens(maxRpm: number, burstSize: number): void {
  if (maxRpm <= 0) return
  const now = Date.now()
  if (lastRefillTime === 0) {
    // Initialisation : on remplit le bucket avec le burst initial
    tokens = burstSize
    lastRefillTime = now
    return
  }

  const intervalMs = 60_000 / maxRpm          // durée entre 2 tokens (ex: 1500ms pour 40 RPM)
  const elapsed = now - lastRefillTime         // temps écoulé depuis le dernier refill
  const newTokens = Math.floor(elapsed / intervalMs) // nombre de tokens à ajouter
  if (newTokens > 0) {
    tokens = Math.min(burstSize, tokens + newTokens) // on plafonne au burst max
    lastRefillTime += newTokens * intervalMs          // on avance sans perdre de fractions
  }
}

/**
 * Tente de réveiller les prochains waiters dans la queue si des tokens
 * sont disponibles et que le bucket n'est pas en pause.
 *
 * Logique :
 * 1. On rafraîchit d'abord les tokens (refillTokens).
 * 2. Pour chaque waiter en tête de queue :
 *    - Si la requête a été annulée (AbortSignal), on la retire sans consommer de token.
 *    - Si un token est dispo et que le bucket n'est pas en pause → on consomme
 *      1 token et on resolve la promesse suspendue (la requête peut partir).
 *    - Si le bucket est en pause → on arrête de drainer (les autres ticks s'en chargeront).
 */
function drainQueue(): void {
  const maxRpm = getMaxRpm()
  if (maxRpm <= 0) return

  const burstSize = getBurstSize()
  refillTokens(maxRpm, burstSize)

  while (waitQueue.length > 0 && tokens > 0) {
    const now = Date.now()
    // Si on est encore en pause 429, on cesse de drainer jusqu'à l'expiration
    if (pausedUntil > now) break

    const entry = waitQueue[0]!
    if (entry.signal?.aborted) {
      // La requête a été annulée — on la retire sans gaspiller un token
      waitQueue.shift()
      entry.reject(new Error('aborted'))
      continue
    }

    // Token disponible + pas en pause → on dispatch la prochaine requête
    tokens--
    waitQueue.shift()
    entry.resolve()
  }
}

/**
 * Planifie le prochain tick de refill du bucket.
 *
 * Le timer est réglé pour se déclencher dans exactement `60_000 / maxRpm` ms,
 * ce qui correspond à la durée d'un cycle entre deux tokens. Au déclenchement :
 * 1. Si le bucket est encore en pause (429 backoff) → on reschedule après
 *    l'expiration de la pause plutôt qu'au prochain tick normal.
 * 2. Sinon → on draine la queue (drainQueue), et on reschedule si des waiters
 *    sont encore présents.
 *
 * `.unref()` est appelé sur le timer pour ne pas empêcher Node.js de terminer
 * le processus si ce timer est le seul handle actif (important pour les CLIs).
 */
function scheduleNextTick(maxRpm: number): void {
  if (refillTimer !== null) return  // un tick est déjà programmé, on ne double pas

  const intervalMs = Math.ceil(60_000 / maxRpm) // arrondi supérieur pour éviter les ticks prématurés
  refillTimer = setTimeout(() => {
    refillTimer = null
    const pauseRemaining = pausedUntil - Date.now()
    if (pauseRemaining > 0) {
      // Encore en pause 429 — on attend la fin de la pause avant de drainer
      scheduleNextTickAfter(pauseRemaining, maxRpm)
      return
    }
    // Pause expirée (ou jamais en pause) — on tente de drainer la queue
    drainQueue()
    // Si des waiters restent, on relance le cycle de ticks
    if (waitQueue.length > 0) {
      scheduleNextTick(maxRpm)
    }
  }, intervalMs)

  // Ne pas bloquer la sortie du processus si ce timer est le seul handle actif
  if (typeof refillTimer === 'object' && refillTimer !== null && 'unref' in refillTimer) {
    (refillTimer as ReturnType<typeof setTimeout> & { unref(): void }).unref()
  }
}

/**
 * Planifie un tick de réveil après un délai arbitraire (ex: fin d'une pause 429).
 * Utilisé à la place de `scheduleNextTick` quand le délai d'attente est connu
 * à l'avance et diffère du cycle normal de refill.
 */
function scheduleNextTickAfter(delayMs: number, maxRpm: number): void {
  if (refillTimer !== null) return // déjà un timer en cours
  refillTimer = setTimeout(() => {
    refillTimer = null
    drainQueue()
    if (waitQueue.length > 0) {
      scheduleNextTick(maxRpm)
    }
  }, delayMs)
  if (typeof refillTimer === 'object' && refillTimer !== null && 'unref' in refillTimer) {
    (refillTimer as ReturnType<typeof setTimeout> & { unref(): void }).unref()
  }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Acquiert un token du bucket avant d'effectuer une requête vers l'API.
 *
 * Cette fonction est le point d'entrée principal du traffic shaper.
 * Elle est appelée par `withRetry` juste avant chaque tentative d'appel HTTP.
 *
 * Deux chemins d'exécution :
 *
 * ─ Fast path (synchrone) :
 *   Si un token est disponible ET que la queue est vide ET que le bucket n'est
 *   pas en pause → on consomme le token immédiatement et on retourne sans await.
 *   Cas courant en régime normal : la requête part instantanément.
 *
 * ─ Slow path (async, mise en queue) :
 *   Si aucun token n'est disponible ou si le bucket est en pause (429 backoff),
 *   la requête est mise dans la FIFO queue. Elle y reste suspendue jusqu'à ce
 *   qu'un token soit distribué par le prochain tick de refill.
 *   Si la queue est déjà à sa capacité maximale (OPENCLAUDE_QUEUE_MAX), on
 *   lève immédiatement une `QueueFullError` sans bloquer.
 *
 * @param maxRpm - Limite de requêtes par minute (doit être > 0 pour activer)
 * @param signal - AbortSignal optionnel pour annuler l'attente en queue
 * @param _windowMs - Ignoré (conservé pour compatibilité avec l'ancienne API)
 */
export async function checkRateLimit(
  maxRpm: number,
  signal?: AbortSignal,
  _windowMs?: number,  // ancienne signature : ignoré, le bucket gère l'intervalle seul
): Promise<void> {
  if (maxRpm <= 0) return // rate limiting désactivé → passage immédiat

  // La requête est déjà annulée avant même d'entrer en queue : rejet immédiat sans consommer de token
  if (signal?.aborted) {
    throw new Error('aborted')
  }

  const burstSize = getBurstSize()
  const queueMax = getQueueMax()

  // On recalcule les tokens accumulés avant chaque décision
  refillTokens(maxRpm, burstSize)

  const now = Date.now()
  const isPaused = pausedUntil > now // true si on est en backoff 429

  // ── Fast path ──────────────────────────────────────────────────────────────
  if (!isPaused && tokens > 0 && waitQueue.length === 0) {
    // Token disponible et pas de waiter devant nous → passage immédiat
    tokens--
    logForDebugging(
      `[TokenBucket] Token acquired immediately (tokens left: ${tokens}, queue: 0)`,
    )
    return
  }

  // ── Slow path : mise en queue ──────────────────────────────────────────────

  // Queue saturée → rejet immédiat pour éviter une accumulation mémoire
  if (waitQueue.length >= queueMax) {
    throw new QueueFullError(queueMax)
  }


  // Log debug différencié : en pause 429 vs juste pas de token disponible
  const pauseMs = isPaused ? pausedUntil - now : 0
  if (pauseMs > 0) {
    logForDebugging(
      `[TokenBucket] Bucket paused for ${pauseMs}ms (429 backoff). ` +
        `Queue depth: ${waitQueue.length + 1}`,
    )
  } else {
    logForDebugging(
      `[TokenBucket] No token available. Queuing request ` +
        `(queue depth: ${waitQueue.length + 1}/${queueMax})`,
    )
  }

  // On suspend l'exécution ici via une Promise jusqu'à ce qu'un token
  // soit disponible et que le bucket soit hors pause.
  return new Promise<void>((resolve, reject) => {
    const entry: WaitEntry = { resolve, reject, signal }

    // Gestionnaire d'annulation : retire l'entrée de la queue sans token
    const onAbort = () => {
      const idx = waitQueue.indexOf(entry)
      if (idx !== -1) waitQueue.splice(idx, 1)
      reject(new Error('aborted'))
    }
    signal?.addEventListener('abort', onAbort, { once: true })

    // On surcharge resolve/reject pour toujours nettoyer l'écouteur abort,
    // même si la résolution vient du bucket (et non d'un abort)
    entry.resolve = () => {
      signal?.removeEventListener('abort', onAbort)
      resolve()
    }
    entry.reject = (err: Error) => {
      signal?.removeEventListener('abort', onAbort)
      reject(err)
    }

    // Ajout en fin de queue (FIFO)
    waitQueue.push(entry)

    // Lance le timer de réveil :
    // - Si en pause 429 → on attend la fin de la pause avant de réessayer
    // - Sinon → on attend le prochain cycle normal de refill
    if (pauseMs > 0) {
      scheduleNextTickAfter(pauseMs, maxRpm)
    } else {
      scheduleNextTick(maxRpm)
    }
  })
}

/**
 * Notifie le bucket qu'un 429 (Too Many Requests) a été reçu de l'API.
 *
 * Cette fonction est le **feedback loop** entre la couche réseau (`withRetry`)
 * et le traffic shaper. Sans elle, les requêtes en queue continueraient d'être
 * dispatché au rythme normal du bucket même pendant le backoff → cascade de 429.
 *
 * Comportement :
 * - Si `retryAfterMs` est fourni (depuis le header `Retry-After` de la réponse
 *   HTTP), on pause le bucket exactement pour cette durée.
 * - Sinon, on calcule un backoff exponentiel avec full jitter pour éviter
 *   que plusieurs instances se synchronisent et relancent toutes ensemble :
 *   `delay = random(0, min(5min, 2s * 2^consecutiveErrors))`
 * - Si le bucket est déjà en pause (autre 429 concurrent), on prend le max
 *   des deux durées (on n'écourte jamais une pause en cours).
 *
 * @param retryAfterMs - Durée de pause en ms (depuis le header Retry-After, optionnel)
 * @param consecutiveErrors - Nombre d'erreurs 429 consécutives (pour le backoff exponentiel)
 */
export function notifyRateLimited(
  retryAfterMs?: number | null,
  consecutiveErrors = 1,
): void {
  const maxRpm = getMaxRpm()
  if (maxRpm <= 0) return // rate limiting désactivé → rien à faire

  let backoffMs: number
  if (retryAfterMs != null && retryAfterMs > 0) {
    // L'API nous dit exactement combien de temps attendre → on l'honore
    backoffMs = retryAfterMs
  } else {
    // Pas de Retry-After → backoff exponentiel avec full jitter
    // Full jitter : on randomise dans [0, ceiling] pour éviter les thundering herds
    // (toutes les instances qui réessaient en même temps après le même délai fixe)
    const cap = 5 * 60_000      // plafond à 5 minutes
    const base = 2_000           // délai de base : 2 secondes
    const ceiling = Math.min(cap, base * Math.pow(2, consecutiveErrors - 1))
    backoffMs = Math.floor(Math.random() * ceiling)
  }

  // On prend le max pour ne jamais réduire une pause déjà en cours
  const newPause = Date.now() + backoffMs
  if (newPause > pausedUntil) {
    pausedUntil = newPause
    logForDebugging(
      `[TokenBucket] 429 received — bucket paused for ${backoffMs}ms ` +
        `(${(backoffMs / 1000).toFixed(1)}s). ` +
        `Queue depth: ${waitQueue.length}`,
    )
  }

  // Si des requêtes attendent en queue, on planifie un réveil à la fin de la pause
  if (waitQueue.length > 0) {
    scheduleNextTickAfter(backoffMs, maxRpm)
  }
}

/**
 * Retourne le nombre de requêtes actuellement en attente dans la FIFO queue.
 * Utile pour le monitoring et le debug (logguer la pression sur le rate limiter).
 */
export function getQueueDepth(): number {
  return waitQueue.length
}

/**
 * Retourne le nombre de tokens actuellement disponibles dans le bucket.
 * Utile pour le monitoring et le debug.
 */
export function getBucketTokens(): number {
  return tokens
}

/**
 * Réinitialise entièrement l'état du bucket.
 * Utilisé UNIQUEMENT dans les tests pour repartir d'un état propre entre
 * chaque cas de test sans fuites de promesses ou de timers.
 *
 * Comportement :
 * - Remet tokens, lastRefillTime et pausedUntil à 0
 * - Annule le timer de refill en cours
 * - Rejette toutes les promesses suspendues dans la queue (avec 'reset')
 *   pour éviter de laisser des Promises en suspens entre les tests
 */
export function resetRateLimiterForTesting(): void {
  tokens = 0
  lastRefillTime = 0
  pausedUntil = 0
  if (refillTimer !== null) {
    clearTimeout(refillTimer)
    refillTimer = null
  }
  // On rejette explicitement tous les waiters pour éviter les fuites de promesses
  while (waitQueue.length > 0) {
    const entry = waitQueue.shift()!
    entry.reject(new Error('reset'))
  }
}
