import { join } from 'path'
import {
    copyFileSync,
    existsSync,
    mkdirSync,
    unlinkSync,
} from 'fs'
import { Prisma } from '@prisma/client'
import { Queue, Worker, Job, ConnectionOptions } from 'bullmq'
import crypto from 'crypto'
import Redis from 'ioredis'

/**
 * Known Caveats:
 * - If a record get's deleted/updated/created from the main database, the block client will not be aware of the change, untill the block is invalidated
 *      - Only to prevent that is to make sure all operation are done on the block client
 * - FindMany operation will still call the main client, to get the latest data
 *     - Although after the fetch, each item will be cached in the block client, so call to retrieve any of the items will return the cached data
 * - aggregate, groupBy, count operations will still call the main client, to get the latest data
 */

export type BlockConfig<T> = {
    /**
     * The id of the block
     */
    blockId: string
    /**
     * The main Prisma client
     * This is the client that will be used to create the block clients
     * It will be used to create the block clients and to sync the main client
     */
    client: T
    /**
     * The queue to add the write operations to
     */
    debug?: boolean

    prismaOptions?: Omit<Prisma.PrismaClientOptions, 'datasources'>
    /**
     * The connection options for the queue
     */
    connection?: {
        host: string
        port: number
        password?: string
    }
    /**
     * The loader function for the block
     */
    loader: (blockClient: T, mainClient: T) => Promise<void>
} & (
        {
            node: 'master'
            ttl?: number
        } | {
            node?: 'worker'
        }
    )


interface WatchOptions<T> {
    ttl?: number
    interval?: number
    /**
     * Interval in milliseconds to check for orphaned queues (blocks with items but no workers)
     * Default: 30000 (30 seconds)
     */
    orphanedQueueCheckInterval?: number
    /**
     * Main Prisma client to use for orphaned queue recovery
     * Required for processing orphaned operations when Block.mainClients is empty
     */
    mainClient: T
    connection?: ConnectionOptions
}

export class Block {
    private static blockClientsWithHooks = new Map<
        string,
        Prisma.DefaultPrismaClient
    >()
    public static blockClients = new Map<string, Prisma.DefaultPrismaClient>()
    private static mainClients = new Map<string, Prisma.DefaultPrismaClient>()

    // Global error handlers setup flag
    private static globalHandlersSetup = false
    private static blockQueues = new Map<string, Queue>()
    private static blockWorkers = new Map<string, Worker>()
    private static locks = new Map<string, Promise<void>>()
    private static redis: Redis | null = null
    private static debug = false
    // Track active write operations separately (these DO block reloads)
    private static activeWriteOperations = new Map<string, number>()
    // Track blocks currently being invalidated
    private static invalidatingBlocks = new Set<string>()
    // Store loader functions for each block
    private static blockLoaders = new Map<string, (blockClient: any, mainClient: any) => Promise<void>>()
    // Track last update time to avoid excessive Redis calls
    private static lastSeenUpdateTimes = new Map<string, number>()
    // Minimum interval between last_seen updates (5 seconds)
    private static LAST_SEEN_UPDATE_INTERVAL = 5000
    // Cache lock status to avoid excessive Redis checks
    private static lockStatusCache = new Map<string, { isLocked: boolean, lastChecked: number }>()
    private static LOCK_CACHE_TTL = 500 // 1 second cache for lock status
    // Track ongoing recovery workers to prevent duplicates
    private static ongoingRecoveryWorkers = new Set<string>()
    // Recovery main clients (separate from normal main clients to avoid conflicts)
    private static recoveryMainClients = new Map<string, Prisma.DefaultPrismaClient>()
    // Batch processing limits
    private static MAX_BATCH_SIZE = 500 // Increased significantly for better throughput
    private static MIN_BATCH_SIZE = 50 // Minimum batch size
    private static TRANSACTION_TIMEOUT = 30000 // 30 seconds transaction timeout
    private static ADAPTIVE_BATCH_SIZE = true // Enable adaptive batch sizing based on performance
    // Adaptive batch sizing tracking
    private static batchPerformanceHistory = new Map<string, { size: number, duration: number, success: boolean }[]>()
    private static PERFORMANCE_HISTORY_SIZE = 20 // Keep last 20 batch performance records
    private static batchSizeLastExplored = new Map<string, number>() // Track last exploration time
    private static EXPLORATION_INTERVAL = 15000 // Explore new batch sizes every 15 seconds
    private static batchSizeLastLogged = new Map<string, number>() // Track last log time
    private static LOG_INTERVAL = 10000 // Log optimal batch size every 10 seconds max
    // Error handling
    private static MAX_OPERATION_RETRIES = 3 // Maximum retries for failed operations
    private static FAILED_OPERATIONS_KEY_PREFIX = 'failed_operations' // Redis key prefix for failed operations
    // WAL maintenance
    private static walMaintenanceInterval: NodeJS.Timeout | null = null
    private static walMaintenanceEnabled = true // Auto-enable WAL maintenance
    private static walHealthCheckInterval: NodeJS.Timeout | null = null
    private static autoRecoveryEnabled = true // Auto-recovery from WAL issues

    static setDebug(debug: boolean) {
        Block.debug = debug
    }

    /**
     * Configure automatic WAL optimizations (optional - enabled by default)
     */
    static configureWalOptimizations(options: {
        maintenance?: boolean      // Auto WAL maintenance (default: true)
        healthMonitoring?: boolean // Auto health monitoring (default: true) 
        autoRecovery?: boolean     // Auto error recovery (default: true)
    }) {
        if (options.maintenance !== undefined) {
            Block.walMaintenanceEnabled = options.maintenance
        }
        if (options.healthMonitoring !== undefined || options.autoRecovery !== undefined) {
            Block.autoRecoveryEnabled = options.autoRecovery ?? options.healthMonitoring ?? true
        }

        if (Block.debug) {
            console.log('[Sharded] WAL configuration updated:', {
                maintenance: Block.walMaintenanceEnabled,
                autoRecovery: Block.autoRecoveryEnabled
            })
        }
    }

    /**
     * Setup global process handlers for unhandled rejections and exceptions
     */
    private static setupGlobalErrorHandlers(): void {
        if (Block.globalHandlersSetup) {
            return
        }

        // Handle unhandled promise rejections
        process.on('unhandledRejection', (reason, promise) => {
            console.error('[Sharded] Unhandled Promise Rejection at:', promise)
            console.error('[Sharded] Reason:', reason)

            if (Block.debug) {
                console.error('[Sharded] Stack trace:', reason instanceof Error ? reason.stack : 'No stack trace available')
            }

            // Don't exit the process, just log the error
            // In production, you might want to implement more sophisticated error reporting
        })

        // Handle uncaught exceptions
        process.on('uncaughtException', (error) => {
            console.error('[Sharded] Uncaught Exception:', error)

            if (Block.debug) {
                console.error('[Sharded] Stack trace:', error.stack)
            }

            // For uncaught exceptions, we typically want to exit gracefully
            console.error('[Sharded] Process will exit due to uncaught exception')
            process.exit(1)
        })

        // Handle process termination gracefully
        process.on('SIGTERM', () => {
            console.log('[Sharded] SIGTERM received, shutting down gracefully...')
            Block.gracefulShutdown(5000).then(() => {
                process.exit(0)
            }).catch(err => {
                console.error('[Sharded] Error during graceful shutdown:', err)
                process.exit(1)
            })
        })

        process.on('SIGINT', () => {
            console.log('[Sharded] SIGINT received, shutting down gracefully...')
            Block.gracefulShutdown(5000).then(() => {
                process.exit(0)
            }).catch(err => {
                console.error('[Sharded] Error during graceful shutdown:', err)
                process.exit(1)
            })
        })

        Block.globalHandlersSetup = true

        if (Block.debug) {
            console.log('[Sharded] Global error handlers setup completed')
        }
    }

    /**
     * Verify that the database path is absolute and consistent across processes
     * This is critical for WAL mode to work correctly
     */
    static async verifyDatabasePath(
        client: any,
        expectedPath: string,
        blockId: string
    ): Promise<void> {
        try {
            // Get the actual database path from SQLite
            const dbList = await client.$queryRaw`SELECT * FROM pragma_database_list;`
            const journalMode = await client.$queryRaw`PRAGMA journal_mode;`

            if (Block.debug) {
                console.log(`[Sharded] Database verification for ${blockId}:`)
                console.log(`[Sharded] Expected path: ${expectedPath}`)
                console.log(`[Sharded] Database list:`, dbList)
                console.log(`[Sharded] Journal mode:`, journalMode)
            }

            // Check that we're in WAL mode
            const walEnabled = journalMode.some((row: any) =>
                row.journal_mode?.toString().toLowerCase() === 'wal'
            )

            if (!walEnabled) {
                throw new Error(`WAL mode not enabled for block ${blockId}`)
            }

            // Verify the file path matches what we expect
            const mainDb = dbList.find((db: any) => db.seq === 0 || db.name === 'main')
            if (mainDb) {
                const actualPath = mainDb.file
                if (actualPath !== expectedPath) {
                    console.warn(`[Sharded] Path mismatch for ${blockId}:`)
                    console.warn(`[Sharded] Expected: ${expectedPath}`)
                    console.warn(`[Sharded] Actual: ${actualPath}`)

                    // If it's just a relative vs absolute path issue, log but don't fail
                    const resolvedActual = require('path').resolve(actualPath)
                    if (resolvedActual !== expectedPath) {
                        throw new Error(
                            `Database path mismatch for ${blockId}: expected ${expectedPath}, got ${actualPath}`
                        )
                    }
                }
            }

            console.log(`[Sharded] ✓ Database path verified for ${blockId}: ${expectedPath}`)
        } catch (err) {
            console.error(`[Sharded] Database path verification failed for ${blockId}:`, err)
            throw err
        }
    }

    /**
     * Get comprehensive WAL diagnostics for a block
     */
    static async getWalDiagnostics(blockId: string): Promise<any> {
        const blockClient = Block.blockClients.get(blockId)
        if (!blockClient) {
            return { error: 'Block client not found' }
        }

        try {
            const [
                journalMode,
                dbList,
                walCheckpoint,
                synchronous,
                busyTimeout,
                cacheSize,
                walAutocheckpoint
            ] = await Promise.all([
                blockClient.$queryRaw`PRAGMA journal_mode;`,
                blockClient.$queryRaw`SELECT * FROM pragma_database_list;`,
                blockClient.$queryRaw`PRAGMA wal_checkpoint(PASSIVE);`,
                blockClient.$queryRaw`PRAGMA synchronous;`,
                blockClient.$queryRaw`PRAGMA busy_timeout;`,
                blockClient.$queryRaw`PRAGMA cache_size;`,
                blockClient.$queryRaw`PRAGMA wal_autocheckpoint;`
            ])

            return {
                blockId,
                timestamp: new Date().toISOString(),
                database_path: (dbList as any[]).find((db: any) => db.seq === 0)?.file,
                journal_mode: (journalMode as any[])[0]?.journal_mode,
                synchronous: (synchronous as any[])[0]?.synchronous,
                busy_timeout: (busyTimeout as any[])[0]?.busy_timeout,
                cache_size: (cacheSize as any[])[0]?.cache_size,
                wal_autocheckpoint: (walAutocheckpoint as any[])[0]?.wal_autocheckpoint,
                wal_checkpoint_result: {
                    busy: (walCheckpoint as any[])[0]?.busy,
                    log: (walCheckpoint as any[])[0]?.log,
                    checkpointed: (walCheckpoint as any[])[0]?.checkpointed
                }
            }
        } catch (err) {
            return {
                blockId,
                error: err instanceof Error ? err.message : String(err),
                timestamp: new Date().toISOString()
            }
        }
    }

    /**
     * Force a WAL checkpoint for a specific block
     */
    static async forceWalCheckpoint(blockId: string, mode: 'PASSIVE' | 'FULL' | 'RESTART' | 'TRUNCATE' = 'FULL'): Promise<any> {
        const blockClient = Block.blockClients.get(blockId)
        if (!blockClient) {
            throw new Error(`Block client not found for ${blockId}`)
        }

        try {
            // Build the PRAGMA statement as a plain string and use $queryRawUnsafe for dynamic parameters
            const result = await blockClient.$queryRawUnsafe(`PRAGMA wal_checkpoint(${mode})`)

            if (Block.debug) {
                console.log(`[Sharded] WAL checkpoint (${mode}) result for ${blockId}:`, result)
            }

            return {
                blockId,
                mode,
                result: (result as any[])[0],
                timestamp: new Date().toISOString()
            }
        } catch (err) {
            console.error(`[Sharded] WAL checkpoint failed for ${blockId}:`, err)
            throw err
        }
    }

    /**
     * Initialize periodic WAL maintenance for all blocks
     */
    static async startWalMaintenance(intervalMs: number = 30000): Promise<void> {
        if (Block.walMaintenanceInterval) {
            clearInterval(Block.walMaintenanceInterval)
        }

        Block.walMaintenanceInterval = setInterval(() => {
            // Wrap async operations to properly handle unhandled rejections
            (async () => {
                try {
                    const blockIds = Array.from(Block.blockClients.keys())

                    for (const blockId of blockIds) {
                        try {
                            // Perform passive checkpoint to avoid blocking operations
                            const result = await Block.forceWalCheckpoint(blockId, 'PASSIVE')

                            if (result.result.busy > 0) {
                                if (Block.debug) {
                                    console.log(`[Sharded] WAL checkpoint busy for ${blockId}, readers may be holding old snapshots`)
                                }
                            }

                            // If checkpoint shows significant WAL size, try a more aggressive checkpoint
                            if (result.result.log > 10000) { // More than 10k pages
                                if (Block.debug) {
                                    console.log(`[Sharded] Large WAL detected for ${blockId} (${result.result.log} pages), attempting FULL checkpoint`)
                                }
                                await Block.forceWalCheckpoint(blockId, 'FULL')
                            }

                        } catch (err) {
                            console.error(`[Sharded] WAL maintenance failed for ${blockId}:`, err)
                            // Continue with other blocks
                        }
                    }
                } catch (err) {
                    console.error('[Sharded] WAL maintenance error:', err)
                }
            })().catch(err => {
                console.error('[Sharded] Unhandled error in WAL maintenance interval:', err)
            })
        }, intervalMs)

        if (Block.debug) {
            console.log(`[Sharded] WAL maintenance started with ${intervalMs}ms interval`)
        }
    }

    /**
     * Stop WAL maintenance
     */
    static stopWalMaintenance(): void {
        if (Block.walMaintenanceInterval) {
            clearInterval(Block.walMaintenanceInterval)
            Block.walMaintenanceInterval = null
            if (Block.debug) {
                console.log('[Sharded] WAL maintenance stopped')
            }
        }

        if (Block.walHealthCheckInterval) {
            clearInterval(Block.walHealthCheckInterval)
            Block.walHealthCheckInterval = null
            if (Block.debug) {
                console.log('[Sharded] WAL health monitoring stopped')
            }
        }
    }

    /**
     * Start automatic health monitoring and recovery (internal - runs automatically)
     */
    private static async startWalHealthMonitoring(): Promise<void> {
        if (Block.walHealthCheckInterval || !Block.autoRecoveryEnabled) {
            return
        }

        Block.walHealthCheckInterval = setInterval(() => {
            // Wrap async operations to properly handle unhandled rejections
            (async () => {
                try {
                    const healthStatus = await Block.getWalHealthStatus()

                    // Auto-recovery for common issues
                    for (const [blockId, blockData] of Object.entries(healthStatus.blocks)) {
                        const data = blockData as any

                        // Handle busy readers automatically
                        if (data.warnings?.some((w: string) => w.includes('Busy readers'))) {
                            if (Block.debug) {
                                console.log(`[Sharded] Auto-recovery: Clearing busy readers for block ${blockId}`)
                            }
                            try {
                                await Block.forceWalCheckpoint(blockId, 'FULL')
                            } catch (err) {
                                if (Block.debug) {
                                    console.log(`[Sharded] Auto-recovery checkpoint failed for ${blockId}:`, err)
                                }
                            }
                        }

                        // Handle large WAL files automatically
                        if (data.warnings?.some((w: string) => w.includes('Large WAL file'))) {
                            if (Block.debug) {
                                console.log(`[Sharded] Auto-recovery: Truncating large WAL for block ${blockId}`)
                            }
                            try {
                                await Block.forceWalCheckpoint(blockId, 'RESTART')
                            } catch (err) {
                                if (Block.debug) {
                                    console.log(`[Sharded] Auto-recovery WAL restart failed for ${blockId}:`, err)
                                }
                            }
                        }
                    }

                    // Log health summary if there are issues
                    if (healthStatus.warning_blocks > 0 || healthStatus.error_blocks > 0) {
                        if (Block.debug) {
                            console.log(`[Sharded] WAL Health: ${healthStatus.healthy_blocks} healthy, ${healthStatus.warning_blocks} warnings, ${healthStatus.error_blocks} errors`)
                        }
                    }

                } catch (err) {
                    // Don't let health monitoring errors break the system
                    if (Block.debug) {
                        console.log('[Sharded] WAL health monitoring error:', err)
                    }
                }
            })().catch(err => {
                console.error('[Sharded] Unhandled error in WAL health monitoring interval:', err)
            })
        }, 60000) // Check every minute

        if (Block.debug) {
            console.log('[Sharded] WAL health monitoring and auto-recovery started')
        }
    }

    /**
     * Get WAL health status for all blocks
     */
    static async getWalHealthStatus(): Promise<any> {
        const blockIds = Array.from(Block.blockClients.keys())
        const healthStatus: any = {
            timestamp: new Date().toISOString(),
            total_blocks: blockIds.length,
            healthy_blocks: 0,
            warning_blocks: 0,
            error_blocks: 0,
            blocks: {}
        }

        for (const blockId of blockIds) {
            try {
                const diagnostics = await Block.getWalDiagnostics(blockId)

                let status = 'healthy'
                const warnings = []

                // Check for potential issues
                if (diagnostics.wal_checkpoint_result?.busy > 0) {
                    warnings.push('Busy readers detected (possible long-lived transactions)')
                    status = 'warning'
                }

                if (diagnostics.wal_checkpoint_result?.log > 5000) {
                    warnings.push(`Large WAL file (${diagnostics.wal_checkpoint_result.log} pages)`)
                    status = 'warning'
                }

                if (diagnostics.journal_mode !== 'wal') {
                    warnings.push('Not in WAL mode')
                    status = 'error'
                }

                if (diagnostics.synchronous < 1) {
                    warnings.push('Synchronous mode below NORMAL (data loss risk)')
                    status = 'warning'
                }

                healthStatus.blocks[blockId] = {
                    status,
                    warnings,
                    diagnostics
                }

                if (status === 'healthy') healthStatus.healthy_blocks++
                else if (status === 'warning') healthStatus.warning_blocks++
                else healthStatus.error_blocks++

            } catch (err) {
                healthStatus.blocks[blockId] = {
                    status: 'error',
                    error: err instanceof Error ? err.message : String(err)
                }
                healthStatus.error_blocks++
            }
        }

        return healthStatus
    }

    /**
     * Run comprehensive WAL diagnostics as per the troubleshooting checklist
     * This implements the "Fast diagnostics" section from the requirements
     */
    static async runWalDiagnostics(blockId?: string): Promise<any> {
        const blockIds = blockId ? [blockId] : Array.from(Block.blockClients.keys())
        const results: any = {
            timestamp: new Date().toISOString(),
            summary: {
                total_blocks: blockIds.length,
                healthy: 0,
                warnings: 0,
                errors: 0
            },
            blocks: {},
            recommendations: []
        }

        for (const id of blockIds) {
            const blockClient = Block.blockClients.get(id)
            if (!blockClient) {
                results.blocks[id] = {
                    status: 'ERROR',
                    error: 'Block client not found'
                }
                results.summary.errors++
                continue
            }

            try {
                // Run the diagnostic queries from the checklist
                const [
                    journalMode,
                    dbList,
                    walCheckpoint,
                    busyTimeout
                ] = await Promise.all([
                    blockClient.$queryRaw`PRAGMA journal_mode;`,
                    blockClient.$queryRaw`SELECT * FROM pragma_database_list;`,
                    blockClient.$queryRaw`PRAGMA wal_checkpoint(PASSIVE);`,
                    blockClient.$queryRaw`PRAGMA busy_timeout;`
                ])

                const diagnostics = {
                    journal_mode: (journalMode as any[])[0]?.journal_mode,
                    database_path: (dbList as any[]).find((db: any) => db.seq === 0)?.file,
                    busy_timeout: (busyTimeout as any[])[0]?.busy_timeout,
                    wal_checkpoint: {
                        busy: (walCheckpoint as any[])[0]?.busy,
                        log: (walCheckpoint as any[])[0]?.log,
                        checkpointed: (walCheckpoint as any[])[0]?.checkpointed
                    }
                }

                // Analyze results and provide recommendations
                const issues = []
                const warnings = []

                // Check #1: Are we in WAL mode?
                if (diagnostics.journal_mode !== 'wal') {
                    issues.push('Not in WAL mode')
                }

                // Check #2: Is the database path absolute?
                if (diagnostics.database_path && !require('path').isAbsolute(diagnostics.database_path)) {
                    warnings.push('Database path is not absolute - this can cause "different file" issues')
                }

                // Check #3: Are there busy readers (long-lived transactions)?
                if (diagnostics.wal_checkpoint.busy > 0) {
                    warnings.push(`${diagnostics.wal_checkpoint.busy} busy readers detected - possible long-lived transactions`)
                    results.recommendations.push(`Kill long-lived readers for block ${id}`)
                }

                // Check #4: Is WAL file growing large?
                if (diagnostics.wal_checkpoint.log > 5000) {
                    warnings.push(`Large WAL file: ${diagnostics.wal_checkpoint.log} pages`)
                    results.recommendations.push(`Consider manual checkpoint for block ${id}`)
                }

                // Check #5: Is busy timeout set appropriately?
                if (diagnostics.busy_timeout < 60000) {
                    warnings.push(`Low busy timeout: ${diagnostics.busy_timeout}ms (recommended: 60000ms for heavy load)`)
                }

                const status = issues.length > 0 ? 'ERROR' : warnings.length > 0 ? 'WARNING' : 'HEALTHY'

                results.blocks[id] = {
                    status,
                    diagnostics,
                    issues,
                    warnings
                }

                if (status === 'HEALTHY') results.summary.healthy++
                else if (status === 'WARNING') results.summary.warnings++
                else results.summary.errors++

            } catch (err) {
                results.blocks[id] = {
                    status: 'ERROR',
                    error: err instanceof Error ? err.message : String(err)
                }
                results.summary.errors++
            }
        }

        // Add general recommendations based on findings
        if (results.summary.warnings > 0 || results.summary.errors > 0) {
            results.recommendations.push(
                'Run `Block.getWalHealthStatus()` for detailed health monitoring',
                'Consider calling `Block.forceWalCheckpoint(blockId, "FULL")` to clear busy readers',
                'Ensure all processes use absolute paths in connection strings',
                'Keep transactions short to prevent long-lived readers'
            )
        }

        return results
    }

    /**
     * Calculate optimal batch size based on performance history
     */
    static getOptimalBatchSize(blockId: string): number {
        if (!Block.ADAPTIVE_BATCH_SIZE) {
            return Block.MAX_BATCH_SIZE
        }

        const now = Date.now()
        const history = Block.batchPerformanceHistory.get(blockId) || []

        // Start with a conservative initial size and scale up systematically
        if (history.length < 3) {
            return 50 // Start at 50 operations as requested
        }

        // Calculate success rate and performance metrics for different batch sizes
        const recentHistory = history.slice(-Block.PERFORMANCE_HISTORY_SIZE)
        const successfulBatches = recentHistory.filter(h => h.success)
        const recentFailures = recentHistory.filter(h => !h.success)

        if (successfulBatches.length === 0) {
            return Block.MIN_BATCH_SIZE // Fall back to minimum if all recent batches failed
        }

        // Check for recent timeout-related failures and be more conservative
        const recentTimeoutFailures = recentFailures.filter(h => h.duration >= Block.TRANSACTION_TIMEOUT * 0.9)
        const hasRecentTimeouts = recentTimeoutFailures.length > 0

        // Group successful batches by size to understand performance per size
        const sizePerformance = new Map<number, { count: number, avgDuration: number }>()

        for (const record of successfulBatches) {
            const existing = sizePerformance.get(record.size)
            if (existing) {
                existing.count++
                existing.avgDuration = (existing.avgDuration * (existing.count - 1) + record.duration) / existing.count
            } else {
                sizePerformance.set(record.size, { count: 1, avgDuration: record.duration })
            }
        }

        // Find the optimal size based on throughput (operations per second)
        // Apply penalties for very small batches to avoid bias toward overhead-light operations
        let bestEfficiency = 0
        let optimalSize = Block.MIN_BATCH_SIZE
        const performanceThreshold = Block.TRANSACTION_TIMEOUT * 0.6 // Use 60% of timeout as threshold

        for (const [size, perf] of sizePerformance) {
            if (perf.avgDuration < performanceThreshold) {
                const throughput = size / (perf.avgDuration / 1000) // operations per second

                // Apply a size-based efficiency multiplier to favor larger batches
                // Small batches get penalized because they don't amortize overhead well
                let sizeMultiplier = 1.0
                if (size <= 3) {
                    sizeMultiplier = 0.6 // Heavily penalize very small batches
                } else if (size <= 8) {
                    sizeMultiplier = 0.8 // Moderately penalize small batches
                } else if (size >= 15) {
                    sizeMultiplier = 1.2 // Favor larger batches
                }

                // Also consider sample size - prefer sizes with more data points
                const sampleWeight = Math.min(1.0, perf.count / 3) // Full weight after 3+ samples

                const efficiency = throughput * sizeMultiplier * sampleWeight

                if (efficiency > bestEfficiency) {
                    bestEfficiency = efficiency
                    optimalSize = size
                }
            }
        }

        // Check if we should explore a larger batch size
        const lastExplored = Block.batchSizeLastExplored.get(blockId) || 0
        const shouldExplore = (now - lastExplored) > Block.EXPLORATION_INTERVAL

        const recentSuccessRate = successfulBatches.length / recentHistory.length
        const maxTestedSize = Math.max(...successfulBatches.map(b => b.size))

        // Aggressive exploration logic: systematically push toward performance ceiling
        // But be much more conservative if we've had recent timeouts
        if (shouldExplore && recentSuccessRate >= 0.7 && maxTestedSize < Block.MAX_BATCH_SIZE && !hasRecentTimeouts) {
            let explorationSize: number

            // More aggressive scaling based on current performance
            if (recentSuccessRate >= 0.95) {
                // Excellent success rate - make large jumps to find ceiling quickly
                explorationSize = Math.min(Block.MAX_BATCH_SIZE, Math.floor(maxTestedSize * 1.5))
            } else if (recentSuccessRate >= 0.9) {
                // Very good success rate - moderate jumps
                explorationSize = Math.min(Block.MAX_BATCH_SIZE, Math.floor(maxTestedSize * 1.3))
            } else if (recentSuccessRate >= 0.8) {
                // Good success rate - conservative jumps
                explorationSize = Math.min(Block.MAX_BATCH_SIZE, maxTestedSize + 25)
            } else {
                // Decent success rate - small jumps
                explorationSize = Math.min(Block.MAX_BATCH_SIZE, maxTestedSize + 15)
            }

            // Ensure we always make meaningful progress
            if (explorationSize <= maxTestedSize) {
                explorationSize = Math.min(Block.MAX_BATCH_SIZE, maxTestedSize + 20)
            }

            Block.batchSizeLastExplored.set(blockId, now)
            optimalSize = explorationSize

            if (Block.debug) {
                const lastLogged = Block.batchSizeLastLogged.get(blockId) || 0
                if ((now - lastLogged) > Block.LOG_INTERVAL) {
                    // Check if we're seeing performance degradation with larger sizes
                    const recentLargeBatches = successfulBatches
                        .filter(b => b.size >= maxTestedSize * 0.8)
                        .sort((a, b) => b.size - a.size)

                    let ceilingWarning = ""
                    if (recentLargeBatches.length >= 3) {
                        const largest = recentLargeBatches[0]
                        const secondLargest = recentLargeBatches[1]
                        const thirdLargest = recentLargeBatches[2]

                        // Check if throughput is declining with larger sizes
                        const largestThroughput = largest.size / (largest.duration / 1000)
                        const secondThroughput = secondLargest.size / (secondLargest.duration / 1000)
                        const thirdThroughput = thirdLargest.size / (thirdLargest.duration / 1000)

                        if (largestThroughput < secondThroughput && secondThroughput < thirdThroughput) {
                            ceilingWarning = " ⚠️ Performance degrading with size - approaching ceiling"
                            // When hitting ceiling, be less aggressive in exploration
                            optimalSize = Math.min(optimalSize, maxTestedSize + 5)
                        } else if (largestThroughput > thirdThroughput * 1.1) {
                            ceilingWarning = " 🚀 Still gaining performance - pushing higher"
                        }
                    }

                    console.log(`[Sharded] Aggressively exploring larger batch size for ${blockId}: ${explorationSize} (max tested: ${maxTestedSize}, success rate: ${recentSuccessRate.toFixed(2)})${ceilingWarning}`)
                    Block.batchSizeLastLogged.set(blockId, now)
                }
            }
        } else if (hasRecentTimeouts) {
            // Special handling for recent timeout failures - scale down aggressively
            const largestSuccessfulSize = Math.max(...successfulBatches.map(b => b.size))
            const timeoutBatchSizes = recentTimeoutFailures.map(f => f.size).filter(s => s > 0)
            const avgTimeoutSize = timeoutBatchSizes.length > 0 ?
                Math.round(timeoutBatchSizes.reduce((sum, size) => sum + size, 0) / timeoutBatchSizes.length) : 0

            // Use a size well below the timeout threshold
            optimalSize = Math.max(Block.MIN_BATCH_SIZE, Math.floor(largestSuccessfulSize * 0.7))

            if (Block.debug) {
                const lastLogged = Block.batchSizeLastLogged.get(blockId) || 0
                if ((now - lastLogged) > Block.LOG_INTERVAL) {
                    console.log(`[Sharded] Recent timeout detected (avg timeout size: ${avgTimeoutSize}), scaling down to ${optimalSize} (was ${largestSuccessfulSize})`)
                    Block.batchSizeLastLogged.set(blockId, now)
                }
            }
        } else {
            // Scale based on recent performance trends
            const avgDuration = successfulBatches.reduce((sum, b) => sum + b.duration, 0) / successfulBatches.length

            // Find the largest successful batch size as a reference point
            const maxSuccessfulSize = Math.max(...successfulBatches.map(b => b.size))
            const minReasonableSize = Math.max(Block.MIN_BATCH_SIZE, Math.floor(maxSuccessfulSize * 0.5))

            if (recentSuccessRate >= 0.95 && avgDuration < performanceThreshold * 0.3) {
                // Excellent performance with fast completion - scale up very aggressively
                optimalSize = Math.min(Block.MAX_BATCH_SIZE, Math.floor(optimalSize * 1.6))
            } else if (recentSuccessRate >= 0.95 && avgDuration < performanceThreshold * 0.5) {
                // Excellent performance - scale up aggressively
                optimalSize = Math.min(Block.MAX_BATCH_SIZE, Math.floor(optimalSize * 1.4))
            } else if (recentSuccessRate >= 0.9 && avgDuration < performanceThreshold * 0.6) {
                // Very good performance - moderate aggressive scaling
                optimalSize = Math.min(Block.MAX_BATCH_SIZE, Math.floor(optimalSize * 1.2))
            } else if (recentSuccessRate >= 0.8 && avgDuration < performanceThreshold * 0.7) {
                // Good performance - conservative scaling
                optimalSize = Math.min(Block.MAX_BATCH_SIZE, optimalSize + 10)
            } else if (recentSuccessRate < 0.7) {
                // Poor performance - scale down, but not below half of max successful size
                optimalSize = Math.max(minReasonableSize, Math.floor(optimalSize * 0.8))
            }

            // Ensure we don't go below a reasonable minimum based on recent success
            optimalSize = Math.max(minReasonableSize, optimalSize)

            // Throttled logging
            if (Block.debug) {
                const lastLogged = Block.batchSizeLastLogged.get(blockId) || 0
                if ((now - lastLogged) > Block.LOG_INTERVAL) {
                    console.log(`[Sharded] Optimal batch size for ${blockId}: ${optimalSize} (success rate: ${recentSuccessRate.toFixed(2)}, efficiency: ${bestEfficiency.toFixed(1)})`)
                    Block.batchSizeLastLogged.set(blockId, now)
                }
            }
        }

        return optimalSize
    }

    /**
     * Record batch performance for adaptive sizing
     */
    static recordBatchPerformance(blockId: string, size: number, duration: number, success: boolean) {
        if (!Block.ADAPTIVE_BATCH_SIZE) return

        let history = Block.batchPerformanceHistory.get(blockId) || []
        history.push({ size, duration, success })

        // Keep only recent history
        if (history.length > Block.PERFORMANCE_HISTORY_SIZE) {
            history = history.slice(-Block.PERFORMANCE_HISTORY_SIZE)
        }

        Block.batchPerformanceHistory.set(blockId, history)

        // Debug logging for performance tracking
        if (Block.debug && !success) {
            console.log(`[Sharded] Batch FAILED for ${blockId}: size=${size}, duration=${duration}ms`)
        }
    }

    /**
     * Get batch performance statistics for debugging
     */
    static getBatchPerformanceStats(blockId: string): any {
        const history = Block.batchPerformanceHistory.get(blockId) || []
        if (history.length === 0) {
            return { message: 'No performance history available' }
        }

        const successful = history.filter(h => h.success)
        const failed = history.filter(h => !h.success)

        // Group by batch size
        const sizeStats = new Map<number, { count: number, avgDuration: number, successRate: number }>()

        for (const record of history) {
            const size = record.size
            const existing = sizeStats.get(size)
            if (existing) {
                const totalDuration = existing.avgDuration * existing.count + record.duration
                existing.count++
                existing.avgDuration = totalDuration / existing.count
                existing.successRate = history.filter(h => h.size === size && h.success).length / history.filter(h => h.size === size).length
            } else {
                sizeStats.set(size, {
                    count: 1,
                    avgDuration: record.duration,
                    successRate: history.filter(h => h.size === size && h.success).length / history.filter(h => h.size === size).length
                })
            }
        }

        return {
            totalBatches: history.length,
            successfulBatches: successful.length,
            failedBatches: failed.length,
            overallSuccessRate: (successful.length / history.length * 100).toFixed(1) + '%',
            avgDurationSuccess: successful.length > 0 ? (successful.reduce((sum, b) => sum + b.duration, 0) / successful.length).toFixed(1) + 'ms' : 'N/A',
            sizeBreakdown: Object.fromEntries(
                Array.from(sizeStats.entries()).map(([size, stats]) => [
                    `size_${size}`,
                    {
                        count: stats.count,
                        avgDuration: stats.avgDuration.toFixed(1) + 'ms',
                        successRate: (stats.successRate * 100).toFixed(1) + '%',
                        throughput: (size / (stats.avgDuration / 1000)).toFixed(1) + ' ops/sec'
                    }
                ])
            )
        }
    }

    /**
     * Update last seen time for a block, but throttled to avoid excessive Redis calls
     */
    static updateLastSeenThrottled(blockId: string) {
        const now = Date.now()
        const lastUpdate = Block.lastSeenUpdateTimes.get(blockId) || 0

        if (now - lastUpdate > Block.LAST_SEEN_UPDATE_INTERVAL) {
            Block.lastSeenUpdateTimes.set(blockId, now)
            // Fire and forget - don't await
            Block.redis?.hset("last_seen", blockId, now).catch(err => {
                if (Block.debug) {
                    console.error('[Sharded] Error updating last_seen:', err)
                }
            })
        }
    }

    static async create<T>(config: BlockConfig<T>) {
        const node = config.node ?? 'worker'

        // Setup global error handlers on first use
        Block.setupGlobalErrorHandlers()

        // Initialize Redis if not already done
        if (!Block.redis) {
            Block.redis = new Redis(config.connection ?? {
                host: process.env.REDIS_HOST ?? 'localhost',
                port: process.env.REDIS_PORT
                    ? parseInt(process.env.REDIS_PORT)
                    : 6379,
                password: process.env.REDIS_PASSWORD,
            })
        }

        // Check for in-process locks (creation/reload)
        if (Block.locks.has(config.blockId)) {
            if (Block.debug) {
                console.log('[Sharded] Block being created/reloaded, waiting...', config.blockId)
            }
            await Block.locks.get(config.blockId)
        }
        Block.debug = config.debug ?? false

        // Check if block already exists in this process - if so, return immediately
        const needsCreation = !Block.blockClientsWithHooks.has(config.blockId) ||
            !Block.blockClients.has(config.blockId) ||
            !Block.mainClients.has(config.blockId)

        // Fast path: if block already exists, return it without any locking
        if (!needsCreation) {
            if (Block.debug) {
                console.log(`[Sharded] Block ${config.blockId} already exists in process, returning existing block`)
            }

            // Update tracking for existing block  
            Block.updateLastSeenThrottled(config.blockId)
            if (config.node === 'master' && config.ttl) {
                Block.redis.hset("block_ttl", config.blockId, config.ttl)
            }

            // Return existing block
            const block = await Block.safeGetBlock<T>(config.blockId)
            if (!block) {
                throw new Error(`Failed to retrieve existing block: ${config.blockId}`)
            }
            return block
        }

        let lockResolve: (() => void) | undefined
        let redisLockAcquired = false

        // Only acquire Redis lock if we actually need to create the block AND we're a master
        if (needsCreation && node === 'master') {
            // Use shared lock for both master creation and reload operations
            const blockLockKey = `block_lock:${config.blockId}`
            const processId = `${process.pid}_${Date.now()}`

            // Try to acquire block lock with 30 second timeout
            const maxWaitTime = 30000
            const startTime = Date.now()

            while (!redisLockAcquired && (Date.now() - startTime < maxWaitTime)) {
                const result = await Block.redis.set(blockLockKey, processId, 'PX', 30000, 'NX')
                if (result === 'OK') {
                    redisLockAcquired = true
                    if (Block.debug) {
                        console.log(`[Sharded] Acquired block lock for master creation: ${config.blockId}`)
                    }
                } else {
                    if (Block.debug) {
                        console.log(`[Sharded] Block lock exists for ${config.blockId}, waiting...`)
                    }
                    await new Promise(resolve => setTimeout(resolve, 100))
                }
            }

            if (!redisLockAcquired) {
                throw new Error(`Failed to acquire block lock for block ${config.blockId} within timeout`)
            }
        }

        if (needsCreation) {
            // Only lock during master creation to prevent multiple masters
            // Workers can be created simultaneously as they just connect to existing SQLite
            if (node === 'master') {
                if (Block.debug) {
                    console.log('[Sharded] Locking master creation of block:', config.blockId)
                }
                Block.locks.set(config.blockId, new Promise(resolve => {
                    lockResolve = resolve
                }))
            }

            if (Block.debug) {
                console.log('[Sharded] Creating new block:', config.blockId)
            }
            const baseDir = join(process.cwd(), 'prisma', 'blocks')
            const dataDir = join(baseDir, 'data')

            if (!existsSync(dataDir)) {
                mkdirSync(dataDir, { recursive: true })
            }

            const generated_client = join(baseDir, 'generated', 'block')
            const { PrismaClient: BlockPrismaClient } = (await import(
                generated_client
            ))

            // Get the template SQLite database path
            const templatePath = join(baseDir, 'template.sqlite')
            if (!existsSync(templatePath)) {
                throw new Error(
                    `Template SQLite database not found at ${templatePath}`,
                )
            }

            // Create buffer database path
            const blockPath = join(dataDir, `${config.blockId}.sqlite`)
            const reload = !existsSync(blockPath)

            if (reload) {
                if (Block.debug) {
                    console.log('[Sharded] Block does not exist, creating:', blockPath)
                }
                copyFileSync(templatePath, blockPath)
            }

            // Create Prisma client for this block
            if (Block.debug) {
                console.log('[Sharded] Creating PrismaClient for block:', blockPath)
            }

            // Use absolute path to ensure all processes connect to the same file
            const absoluteBlockPath = require('path').resolve(blockPath)
            const connectionUrl = `file:${absoluteBlockPath}?mode=rwc&cache=private`

            const blockClient = new BlockPrismaClient({
                datasources: {
                    db: {
                        url: connectionUrl,
                    },
                },
                ...config.prismaOptions,
            })

            // Enable WAL mode and set pragmas for robust multi-process operation
            try {
                // Core WAL configuration
                await blockClient.$queryRaw`PRAGMA journal_mode=WAL`
                await blockClient.$queryRaw`PRAGMA synchronous=FULL` // Use FULL for durability (safer than NORMAL)
                await blockClient.$queryRaw`PRAGMA busy_timeout=60000` // Increased to 60000 (60 seconds) for heavy load

                // Memory and performance settings
                await blockClient.$queryRaw`PRAGMA cache_size=-4000` // Use 4MB of memory for cache
                await blockClient.$queryRaw`PRAGMA temp_store=MEMORY` // Keep temp tables in memory

                // WAL-specific settings for better concurrency and reliability
                await blockClient.$queryRaw`PRAGMA wal_autocheckpoint=500` // Very aggressive checkpointing to prevent large WAL files

                // Skip mmap for better container/filesystem compatibility
                await blockClient.$queryRaw`PRAGMA mmap_size=0`

                // Additional safety settings
                await blockClient.$queryRaw`PRAGMA foreign_keys=ON`
                await blockClient.$queryRaw`PRAGMA secure_delete=ON`

                if (Block.debug) {
                    console.log(`[Sharded] SQLite pragmas configured for ${absoluteBlockPath}`)
                }
            } catch (err) {
                console.error(
                    '[Sharded] Error setting SQLite pragmas:',
                    err,
                )
                throw err // Fail fast if pragmas can't be set
            }

            // Critical: Verify exact database file path to ensure all processes use the same file
            try {
                await Block.verifyDatabasePath(blockClient, absoluteBlockPath, config.blockId)
                if (Block.debug) {
                    console.log(`[Sharded] ✅ WAL optimization enabled for block ${config.blockId}`)
                }
            } catch (err) {
                console.error(
                    '[Sharded] ⚠️ Database path verification failed for block',
                    config.blockId,
                    ':',
                    err,
                )
                // Don't fail completely - log and continue with basic functionality
                console.log('[Sharded] Continuing with basic WAL mode (some optimizations disabled)')
            }

            // Initialize worker queue for this block
            await Block.init_queue(config.blockId, config.connection, node)

            // Track the block client and main client
            Block.blockClients.set(config.blockId, blockClient)
            Block.mainClients.set(
                config.blockId,
                config.client as Prisma.DefaultPrismaClient,
            )
            // Store the loader function for potential reloads
            Block.blockLoaders.set(config.blockId, config.loader)

            // Add hooks to the block client
            const blockClientWithHooks = await Block.add_hooks(
                blockClient,
                config.blockId,
            )
            Block.blockClientsWithHooks.set(
                config.blockId,
                blockClientWithHooks,
            )

            if (reload) {
                if (Block.debug) {
                    console.log('[Sharded] Loading block data:', config.blockId)
                }
                await config.loader(blockClient as T, config.client as T)
                if (Block.debug) {
                    console.log('[Sharded] Block data loaded:', config.blockId)
                }
            }

            if (Block.debug) {
                console.log('[Sharded] Created block:', config.blockId)
            }

            // Start WAL maintenance and health monitoring automatically for master nodes
            if (node === 'master' && Block.blockClients.size === 1) {
                if (Block.walMaintenanceEnabled && !Block.walMaintenanceInterval) {
                    await Block.startWalMaintenance(30000) // 30 second intervals
                }
                if (Block.autoRecoveryEnabled && !Block.walHealthCheckInterval) {
                    await Block.startWalHealthMonitoring()
                }
            }

            // Resolve the in-process lock after everything is complete (including loader)
            if (lockResolve) {
                Block.locks.delete(config.blockId)
                if (Block.debug) {
                    console.log('[Sharded] Unlocking creation of block:', config.blockId)
                }
                lockResolve()
            }

            // Release Redis block lock if we acquired it
            if (redisLockAcquired) {
                const blockLockKey = `block_lock:${config.blockId}`
                await Block.redis?.del(blockLockKey)
                if (Block.debug) {
                    console.log(`[Sharded] Released block lock for ${config.blockId}`)
                }
            }
        }


        if (config.node === 'master' && config.ttl) {
            Block.redis.hset("block_ttl", config.blockId, config.ttl)
        }

        // Tracking block for invalidation
        Block.updateLastSeenThrottled(config.blockId)

        // Safe retrieval with race condition handling
        const block = await Block.safeGetBlock<T>(config.blockId)
        if (!block) {
            throw new Error(`Failed to create or retrieve block: ${config.blockId}. It may have been invalidated during creation.`)
        }

        return block
    }

    /**
     * Wait for all pending sync operations to complete for a given block
     */
    static async waitForPendingSyncs(blockId: string, timeoutMs: number = 30000): Promise<void> {
        if (!Block.redis) {
            if (Block.debug) {
                console.log(`[Sharded] No Redis connection, assuming no pending syncs for block ${blockId}`)
            }
            return
        }

        const startTime = Date.now()
        const operationQueueKey = `block_operations:${blockId}`

        while (Date.now() - startTime < timeoutMs) {
            const pendingCount = await Block.redis.llen(operationQueueKey)

            if (pendingCount === 0) {
                if (Block.debug) {
                    console.log(`[Sharded] All syncs completed for block ${blockId}`)
                }
                return
            }

            if (Block.debug) {
                console.log(`[Sharded] Waiting for ${pendingCount} pending syncs for block ${blockId}, (operationKey: ${operationQueueKey})`)
            }

            await new Promise(resolve => setTimeout(resolve, 100))
        }

        console.warn(`[Sharded] Timeout waiting for pending syncs for block ${blockId}`)
    }

    /**
     * Safely retrieve a block with retry logic to handle race conditions
     */
    static async safeGetBlock<T>(blockId: string, maxRetries = 3): Promise<T | null> {
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            const block = Block.blockClientsWithHooks.get(blockId)
            if (block) {
                // Update last seen before returning
                Block.updateLastSeenThrottled(blockId)
                return block as T
            }

            if (attempt < maxRetries - 1) {
                // Wait a bit before retrying
                await new Promise(resolve => setTimeout(resolve, 100))
            }
        }
        return null
    }

    /**
     * Get all model names from a Prisma client instance
     */
    static getClientModels(client: any): string[] {
        const models: string[] = []

        // Method 1: Try to get from Prisma's internal _dmmf (Data Model Meta Format)
        try {
            if (client._dmmf && client._dmmf.datamodel && client._dmmf.datamodel.models) {
                const dmmfModels = client._dmmf.datamodel.models.map((model: any) => model.name.toLowerCase())
                if (Block.debug) {
                    console.log('[Sharded] Discovered models from DMMF:', dmmfModels)
                }
                return dmmfModels
            }
        } catch (err) {
            if (Block.debug) {
                console.log('[Sharded] Could not get models from _dmmf:', err)
            }
        }

        // Method 2: Try to get from _runtimeDataModel (Prisma 5+)
        try {
            if (client._runtimeDataModel && client._runtimeDataModel.models) {
                const runtimeModels = Object.keys(client._runtimeDataModel.models).map(name => name.toLowerCase())
                if (Block.debug) {
                    console.log('[Sharded] Discovered models from runtime data model:', runtimeModels)
                }
                return runtimeModels
            }
        } catch (err) {
            if (Block.debug) {
                console.log('[Sharded] Could not get models from _runtimeDataModel:', err)
            }
        }

        // Method 3: Inspect client properties to find model delegates
        try {
            const clientKeys = Object.getOwnPropertyNames(client)
            const modelPattern = /^[a-z][a-zA-Z0-9]*$/

            for (const key of clientKeys) {
                // Skip internal properties, methods, and known non-model properties
                if (key.startsWith('_') || key.startsWith('$') || typeof client[key] === 'function') {
                    continue
                }

                // Check if the property looks like a model delegate
                if (modelPattern.test(key) && client[key] && typeof client[key] === 'object') {
                    // Verify it has typical Prisma model methods
                    const delegate = client[key]
                    if (delegate && typeof delegate === 'object' &&
                        delegate.findMany && delegate.create && delegate.update && delegate.delete) {
                        models.push(key)
                    }
                }
            }

            if (models.length > 0) {
                if (Block.debug) {
                    console.log('[Sharded] Discovered models from client inspection:', models)
                }
                return models
            }
        } catch (err) {
            if (Block.debug) {
                console.log('[Sharded] Could not inspect client properties:', err)
            }
        }

        // Method 4: Fallback to common models if other methods fail
        console.warn('[Sharded] Could not dynamically discover models, using fallback list')
        return ['user', 'order'] // Fallback based on your schema - update this list as needed
    }

    /**
     * Reload block data from main database using the configured loader
     */
    static async reloadBlockData(blockId: string): Promise<void> {
        // Use the same shared lock as master creation - they are mutually exclusive
        const blockLockKey = `block_lock:${blockId}`
        const processId = `${process.pid}_${Date.now()}`

        // Try to acquire block lock with 30 second timeout
        let lockAcquired = false
        const maxWaitTime = 30000
        const startTime = Date.now()

        while (!lockAcquired && (Date.now() - startTime < maxWaitTime)) {
            const result = await Block.redis?.set(blockLockKey, processId, 'PX', 30000, 'NX')
            if (result === 'OK') {
                lockAcquired = true
                if (Block.debug) {
                    console.log(`[Sharded] Acquired block lock for reload: ${blockId}`)
                }
            } else {
                if (Block.debug) {
                    console.log(`[Sharded] Block lock exists for ${blockId}, waiting...`)
                }
                await new Promise(resolve => setTimeout(resolve, 100))
            }
        }

        if (!lockAcquired) {
            throw new Error(`Failed to acquire block lock for block ${blockId} within timeout`)
        }

        // Check for in-process locks (creation/reload)
        if (Block.locks.has(blockId)) {
            if (Block.debug) {
                console.log(`[Sharded] Waiting for existing in-process lock during reload: ${blockId}`)
            }
            await Block.locks.get(blockId)
        }

        const blockClient = Block.blockClients.get(blockId)
        const mainClient = Block.mainClients.get(blockId)
        const loader = Block.blockLoaders.get(blockId)

        if (!blockClient || !mainClient || !loader) {
            if (Block.debug) {
                console.log(`[Sharded] Cannot reload block ${blockId}: missing clients or loader`)
            }
            return
        }

        console.log(`[Sharded] Reloading block data for ${blockId}`)

        // Create a lock to prevent concurrent reloads and block creation
        let lockResolve: (() => void) | undefined
        Block.locks.set(blockId, new Promise(resolve => {
            lockResolve = resolve
        }))

        try {
            // Instead of waiting for ALL operations, just wait for write operations to sync
            // Read operations can continue during reload as they'll fallback to main DB
            const maxWaitTime = 5000 // Much shorter wait - just for critical sync operations
            const startTime = Date.now()

            // Wait for critical write operations and pending sync operations
            while (Date.now() - startTime < maxWaitTime) {
                const pendingCount = await Block.redis?.llen(`block_operations:${blockId}`) || 0
                const activeWrites = Block.activeWriteOperations.get(blockId) || 0

                if (pendingCount === 0 && activeWrites === 0) {
                    if (Block.debug) {
                        console.log(`[Sharded] All write operations completed for ${blockId}, proceeding with reload`)
                    }
                    break
                }
                if (Block.debug) {
                    console.log(`[Sharded] Waiting for ${pendingCount} pending syncs + ${activeWrites} active writes before reloading ${blockId}`)
                }
                await new Promise(resolve => setTimeout(resolve, 100))
            }

            if (Block.debug) {
                console.log(`[Sharded] Proceeding with reload of ${blockId} - active read operations can continue concurrently`)
            }

            // Clear existing data before reloading
            // Dynamically get all models from the Prisma client
            const models = Block.getClientModels(blockClient)
            for (const model of models) {
                try {
                    await (blockClient as any)[model].deleteMany({})
                } catch (err) {
                    // Model might not exist or be empty, continue
                    if (Block.debug) {
                        console.log(`[Sharded] Could not clear model ${model} for block ${blockId}:`, err)
                    }
                }
            }

            // Use the original loader function to reload data
            await loader(blockClient, mainClient)

            if (Block.debug) {
                console.log(`[Sharded] Block data reloaded successfully for ${blockId}`)
            }
        } catch (err) {
            console.error(`[Sharded] Error reloading block data for ${blockId}:`, err)
            throw err
        } finally {
            // Always release the in-process lock
            Block.locks.delete(blockId)
            if (lockResolve) {
                lockResolve()
            }

            // Release Redis block lock
            const blockLockKey = `block_lock:${blockId}`
            await Block.redis?.del(blockLockKey)
            if (Block.debug) {
                console.log(`[Sharded] Released block lock for ${blockId}`)
            }
        }
    }

    /**
     * Check if block is locked for creation or reload (cross-process) with caching
     */
    static async waitForBlockReady(blockId: string): Promise<void> {
        // Fast path: check in-process locks first (no Redis call needed)
        if (Block.locks.has(blockId)) {
            await Block.locks.get(blockId)
            return
        }

        // Check cached lock status
        const cached = Block.lockStatusCache.get(blockId)
        const now = Date.now()

        if (cached && (now - cached.lastChecked) < Block.LOCK_CACHE_TTL) {
            if (!cached.isLocked) {
                return // Cache says no lock, proceed immediately
            }
        }

        // Check for Redis block lock (master creation or reload)
        const blockLockKey = `block_lock:${blockId}`
        const blockLockExists = await Block.redis?.exists(blockLockKey)

        // Update cache
        Block.lockStatusCache.set(blockId, {
            isLocked: !!blockLockExists,
            lastChecked: now
        })

        if (!blockLockExists) {
            return // Block is ready for operations
        }

        // If locked, wait with exponential backoff
        const maxWaitTime = 30000
        const startTime = Date.now()
        let backoffDelay = 100

        while (Date.now() - startTime < maxWaitTime) {
            if (Block.debug) {
                console.log(`[Sharded] Waiting for block operation (master creation/reload) to complete for block ${blockId}`)
            }

            await new Promise(resolve => setTimeout(resolve, backoffDelay))

            // Exponential backoff up to 1 second
            backoffDelay = Math.min(backoffDelay * 1.5, 1000)

            const blockLockExists = await Block.redis?.exists(blockLockKey)
            Block.lockStatusCache.set(blockId, {
                isLocked: !!blockLockExists,
                lastChecked: Date.now()
            })

            if (!blockLockExists && !Block.locks.has(blockId)) {
                return // Block is ready for operations
            }
        }

        throw new Error(`Timeout waiting for block ${blockId} to be ready for operations`)
    }

    /**
     * Increment the write operation count for a block (blocks reloads)
     */
    static incrementWriteOperationCount(blockId: string) {
        const current = Block.activeWriteOperations.get(blockId) || 0
        Block.activeWriteOperations.set(blockId, current + 1)
        if (Block.debug) {
            console.log(`[Sharded] Write operation count for ${blockId}: ${current + 1}`)
        }
    }

    /**
     * Decrement the write operation count for a block
     */
    static decrementWriteOperationCount(blockId: string) {
        const current = Block.activeWriteOperations.get(blockId) || 0
        if (current > 1) {
            Block.activeWriteOperations.set(blockId, current - 1)
        } else {
            Block.activeWriteOperations.delete(blockId)
        }
        if (Block.debug) {
            console.log(`[Sharded] Write operation count for ${blockId}: ${Math.max(0, current - 1)}`)
        }
    }

    /**
     * Check if a block can be safely invalidated (no active write operations, creation, or reload)
     */
    static async canInvalidate(blockId: string): Promise<boolean> {
        // Check for active write operations and double invalidation
        if (Block.activeWriteOperations.has(blockId) || Block.invalidatingBlocks.has(blockId)) {
            return false
        }

        // Check for Redis lock (master creation or reload in progress)
        const blockLockKey = `block_lock:${blockId}`
        const lockExists = await Block.redis?.exists(blockLockKey)
        if (lockExists) {
            if (Block.debug) {
                console.log(`[Sharded] Cannot invalidate ${blockId}: block creation/reload in progress`)
            }
            return false
        }

        // Check for in-process locks
        if (Block.locks.has(blockId)) {
            if (Block.debug) {
                console.log(`[Sharded] Cannot invalidate ${blockId}: in-process lock exists`)
            }
            return false
        }

        return true
    }

    static async delete_block(blockId: string) {
        const baseDir = join(process.cwd(), 'prisma', 'blocks', 'data')
        const blockPath = join(baseDir, `${blockId}.sqlite`)
        // Delete SQLite database files using glob pattern
        const suffixes = ['', '-shm', '-wal']
        suffixes.forEach(suffix => {
            const filePath = blockPath + suffix
            if (Block.debug) {
                console.log('[Sharded] Deleting block file:', filePath)
            }
            if (existsSync(filePath)) {
                unlinkSync(filePath)
            }
        })
    }

    static async init_queue(blockId: string, connection?: ConnectionOptions, node?: 'master' | 'worker') {
        // Initialize Redis connection if not already done
        if (!Block.redis) {
            Block.redis = new Redis({
                host: process.env.REDIS_HOST ?? 'localhost',
                port: process.env.REDIS_PORT ? parseInt(process.env.REDIS_PORT) : 6379,
                password: process.env.REDIS_PASSWORD,
                retryStrategy: function (times: number) {
                    return Math.max(Math.min(Math.exp(times), 20000), 1000);
                },
                enableOfflineQueue: false,
            })
        }

        if (node === 'master' && !Block.blockWorkers.has(blockId)) {
            // Create a batch processing worker that runs every 100ms
            const worker = new Worker(`${blockId}_batch`, Block.batch_sync_operation, {
                concurrency: 1,
                connection: connection ?? {
                    host: process.env.REDIS_HOST ?? 'localhost',
                    port: process.env.REDIS_PORT
                        ? parseInt(process.env.REDIS_PORT)
                        : 6379,
                    password: process.env.REDIS_PASSWORD,
                    retryStrategy: function (times: number) {
                        return Math.max(Math.min(Math.exp(times), 20000), 1000);
                    },
                    maxRetriesPerRequest: null,
                    enableOfflineQueue: true,
                },
            })

            worker.on("error", (error) => {
                console.error("[sharded] Unhandled error in block batch sync worker", error);
            });

            // Handling unexpected shutdowns
            process.on('SIGINT', () => {
                console.log(`[sharded] Received SIGINT, shutting down block batch sync worker`);
                worker.close();
            });
            process.on('SIGTERM', () => {
                console.log(`[sharded] Received SIGTERM, shutting down block batch sync worker`);
                worker.close();
            });

            try {
                // Ensure worker is ready
                await worker.waitUntilReady()
                Block.blockWorkers.set(blockId, worker)
            } catch (err) {
                console.error('Error initializing batch worker:', err)
                throw err
            }

            // Create the batch processing queue for this block
            const batchQueue = new Queue(`${blockId}_batch`, {
                connection: connection ?? {
                    host: process.env.REDIS_HOST ?? 'localhost',
                    port: process.env.REDIS_PORT
                        ? parseInt(process.env.REDIS_PORT)
                        : 6379,
                    password: process.env.REDIS_PASSWORD,
                    retryStrategy: function (times: number) {
                        return Math.max(Math.min(Math.exp(times), 20000), 1000);
                    },
                    enableOfflineQueue: false,
                },
                defaultJobOptions: {
                    removeOnComplete: true,
                    removeOnFail: {
                        count: 1000,
                        age: 24 * 3600, // keep up to 24 hours
                    },
                    backoff: {
                        type: 'fixed',
                        delay: 0,
                    },
                    attempts: 3,
                },
            })

            batchQueue.on("error", (error) => {
                console.error(`[sharded][blocks] Unhandled error in batch queue ${batchQueue.name}`, error);
            });

            try {
                await batchQueue.waitUntilReady()
                Block.blockQueues.set(blockId, batchQueue)

                console.log(`[Sharded] Scheduling recurring batch processing every 100ms for ${blockId}`)
                // Schedule recurring batch processing every 100ms
                await batchQueue.upsertJobScheduler(`${blockId}_batch_processor`, {
                    every: 100, // 100ms
                }, {
                    name: `${blockId}_batch_processor`,
                    data: { blockId },
                })
            } catch (err) {
                console.error('Error initializing batch queue:', err)
                throw err
            }
        } else if (node !== 'master') {
            // Worker nodes need to connect to the existing queue created by master
            if (!Block.blockQueues.has(blockId)) {
                // Create a connection to the existing queue (without worker)
                const existingQueue = new Queue(`${blockId}_batch`, {
                    connection: connection ?? {
                        host: process.env.REDIS_HOST ?? 'localhost',
                        port: process.env.REDIS_PORT
                            ? parseInt(process.env.REDIS_PORT)
                            : 6379,
                        password: process.env.REDIS_PASSWORD,
                        retryStrategy: function (times: number) {
                            return Math.max(Math.min(Math.exp(times), 20000), 1000);
                        },
                        enableOfflineQueue: false,
                    },
                })

                try {
                    await existingQueue.waitUntilReady()
                    Block.blockQueues.set(blockId, existingQueue)
                } catch (err) {
                    console.error(`Error connecting to existing queue for worker node:`, err)
                    throw err
                }
            }
        }

        const queue = Block.blockQueues.get(blockId)
        if (!queue) {
            // Provide detailed error information for debugging
            const isWorkerNode = node !== 'master'
            const hasWorker = Block.blockWorkers.has(blockId)
            const redisConnected = Block.redis?.status === 'ready'

            throw new Error(`Failed to initialize queue for block ${blockId}. Debug info: ` +
                `node=${node}, isWorker=${isWorkerNode}, hasWorker=${hasWorker}, ` +
                `redisStatus=${Block.redis?.status}, queueCount=${Block.blockQueues.size}`)
        }
        return queue
    }


    static async batch_sync_operation(
        job: Job<{ blockId: string }>
    ) {
        const { blockId } = job.data
        const startTime = Date.now()

        let timings = {
            total: 0,
            redis_fetch: 0,
            parse: 0,
            batch_transaction: 0,
            individual_processing: 0,
            requeue: 0,
            operations_count: 0,
            batch_success: false,
            individual_operations_count: 0,
        }

        try {
            // Check recovery clients first, then normal main clients
            let mainClient = Block.recoveryMainClients.get(blockId)
            if (!mainClient) {
                mainClient = Block.mainClients.get(blockId)
            }

            if (!mainClient) {
                if (Block.debug) {
                    console.log(`[Sharded] Main client for block ${blockId} not found - block may have been invalidated`)
                }
                return
            }


            // Atomically get and remove operations to prevent race conditions
            const operationQueueKey = `block_operations:${blockId}`

            // Use MULTI/EXEC to atomically get operations and clear the list
            // Use adaptive batch size to prevent transaction timeouts
            const optimalBatchSize = Block.getOptimalBatchSize(blockId)

            const redisStartTime = Date.now()
            const multi = Block.redis?.multi()
            if (!multi) {
                if (Block.debug) {
                    console.log(`[Sharded] Redis multi for block ${blockId} not found`)
                }
                return
            }

            // Get limited number of operations and remove them atomically
            multi.lrange(operationQueueKey, 0, optimalBatchSize - 1)
            multi.ltrim(operationQueueKey, optimalBatchSize, -1)
            const results = await multi.exec()
            timings.redis_fetch = Date.now() - redisStartTime

            if (!results || !results[0] || !results[0][1]) {
                return // No operations to process
            }

            const operations = results[0][1] as string[]
            if (operations.length === 0) {
                return
            }

            if (Block.debug) {
                console.log('🔄 Batch sync operation started')
            }
            timings.operations_count = operations.length

            if (Block.debug) {
                console.log(`[Sharded] Starting batch sync for block ${blockId}, ${operations.length} operations to process`)
            }

            // Parse operations
            const parseStartTime = Date.now()
            const parsedOperations = operations.map(op => {
                try {
                    return JSON.parse(op)
                } catch (err) {
                    console.error('[Sharded] Failed to parse operation:', op, err)
                    return null
                }
            }).filter(op => op !== null)
            timings.parse = Date.now() - parseStartTime

            if (parsedOperations.length === 0) {
                return
            }

            if (Block.debug) {
                console.log(`[Sharded] Processing ${parsedOperations.length} operations for block ${blockId}`)
            }

            // First attempt: Execute all operations in a single batch transaction
            let batchSuccess = false
            const failedOperations: any[] = []

            const batchStartTime = Date.now()
            try {
                await mainClient.$transaction(async (tx) => {
                    for (const { operation, model, args } of parsedOperations) {
                        await (tx as any)[model][operation](args)
                    }
                }, {
                    timeout: Block.TRANSACTION_TIMEOUT,
                    maxWait: Block.TRANSACTION_TIMEOUT,
                })

                batchSuccess = true
                timings.batch_transaction = Date.now() - batchStartTime
                timings.batch_success = true

                // Record successful batch performance
                Block.recordBatchPerformance(blockId, parsedOperations.length, timings.batch_transaction, true)

                if (Block.debug) {
                    console.log(`[Sharded] Batch transaction succeeded for ${parsedOperations.length} operations in ${timings.batch_transaction}ms`)
                }

            } catch (err: any) {
                timings.batch_transaction = Date.now() - batchStartTime

                // Record failed batch performance
                Block.recordBatchPerformance(blockId, parsedOperations.length, timings.batch_transaction, false)

                // Batch failed - need to isolate the problematic operation(s)
                const isTimeout = err.message?.includes('timeout') || err.message?.includes('expired') || err.code === 'P2024'
                if (Block.debug) {
                    console.log(`[Sharded] Batch transaction failed after ${timings.batch_transaction}ms (${isTimeout ? 'TIMEOUT' : 'ERROR'}), isolating operations: ${err.message}`)
                }

                // Try operations individually to identify the bad ones
                const individualStartTime = Date.now()
                let individualCount = 0

                for (let i = 0; i < parsedOperations.length; i++) {
                    const { operation, model, args, retry_count = 0 } = parsedOperations[i]
                    individualCount++

                    try {
                        await mainClient.$transaction(async (tx) => {
                            await (tx as any)[model][operation](args)
                        }, {
                            timeout: 60000, // Aggressive timeout for individual operations under heavy load
                            maxWait: 20000, // Aggressive maxWait for individual ops
                        })

                        // Operation succeeded individually

                    } catch (individualErr: any) {
                        // Handle individual operation failure
                        if (individualErr.code === 'P2002') {
                            // Unique constraint - skip permanently
                            const uniqueField = individualErr.meta?.target?.[0]
                            console.warn(
                                `[Sharded] Duplicate record skipped in ${model} with unique field ${uniqueField}`,
                                { blockId, model, operation }
                            )

                        } else if (individualErr.code === 'P2025') {
                            // Record not found - skip permanently  
                            console.warn(
                                `[Sharded] Record not found for ${model}.${operation}, skipping`,
                                { blockId, model, operation }
                            )
                        } else if (individualErr.code === 'P2024' || individualErr.message?.includes('timeout') || individualErr.message?.includes('expired')) {
                            // Transaction timeout - retry with backoff
                            console.warn(
                                `[Sharded] Transaction timeout for ${model}.${operation}, will retry`,
                                { blockId, model, operation, attempt: retry_count + 1 }
                            )
                            failedOperations.push({
                                ...parsedOperations[i],
                                retry_count: retry_count + 1,
                                last_error: 'Transaction timeout',
                                failed_at: Date.now(),
                            })

                        } else if (retry_count < Block.MAX_OPERATION_RETRIES) {
                            // Retriable error
                            failedOperations.push({
                                ...parsedOperations[i],
                                retry_count: retry_count + 1,
                                last_error: individualErr.message || individualErr.toString(),
                                failed_at: Date.now(),
                            })

                        } else {
                            // Permanent failure after max retries
                            const deadLetterKey = `${Block.FAILED_OPERATIONS_KEY_PREFIX}:${blockId}`
                            const failedOperation = {
                                ...parsedOperations[i],
                                retry_count: retry_count + 1,
                                final_error: individualErr.message || individualErr.toString(),
                                failed_permanently_at: Date.now(),
                            }

                            try {
                                await Block.redis?.lpush(deadLetterKey, JSON.stringify(failedOperation))
                            } catch (deadLetterErr) {
                                console.error(
                                    `[Sharded] Failed to store operation in dead letter queue:`,
                                    { blockId, model, operation, error: deadLetterErr }
                                )
                                // Don't let dead letter queue failures stop the worker
                            }

                            console.error(
                                `[Sharded] Operation permanently failed: ${model}.${operation}`,
                                { blockId, error: individualErr.message }
                            )
                        }
                    }
                }

                timings.individual_processing = Date.now() - individualStartTime
                timings.individual_operations_count = individualCount

                if (Block.debug) {
                    console.log(`[Sharded] Individual processing completed in ${timings.individual_processing}ms for ${individualCount} operations`)
                }
            }

            // Re-queue failed operations for retry
            if (!batchSuccess && failedOperations.length > 0) {
                const reQueueStartTime = Date.now()
                try {
                    const reQueueMulti = Block.redis?.multi()
                    if (reQueueMulti) {
                        for (let i = failedOperations.length - 1; i >= 0; i--) {
                            reQueueMulti.lpush(operationQueueKey, JSON.stringify(failedOperations[i]))
                        }
                        await reQueueMulti.exec()
                    }
                    timings.requeue = Date.now() - reQueueStartTime

                    if (Block.debug) {
                        console.log(`[Sharded] Re-queued ${failedOperations.length} operations for retry in ${timings.requeue}ms`)
                    }
                } catch (reQueueErr) {
                    timings.requeue = Date.now() - reQueueStartTime
                    console.error(
                        `[Sharded] Failed to re-queue ${failedOperations.length} operations for ${blockId}:`,
                        reQueueErr
                    )
                    // Don't stop the worker - continue processing
                }
            }

            // Calculate total time
            timings.total = Date.now() - startTime

            // Log comprehensive timing information with adaptive batch size info
            const currentOptimalSize = Block.getOptimalBatchSize(blockId)
            if (Block.debug) {
                console.log(`[Sharded] Batch sync completed for ${blockId}:`, {
                    total_time: `${timings.total}ms`,
                    operations_processed: timings.operations_count,
                    batch_success: timings.batch_success,
                    adaptive_batch_size: {
                        used_size: timings.operations_count,
                        optimal_size: currentOptimalSize,
                        max_size: Block.MAX_BATCH_SIZE,
                    },
                    timings: {
                        redis_fetch: `${timings.redis_fetch}ms`,
                        parse: `${timings.parse}ms`,
                        batch_transaction: `${timings.batch_transaction}ms`,
                        individual_processing: `${timings.individual_processing}ms`,
                        requeue: `${timings.requeue}ms`,
                    },
                    performance: {
                        ops_per_second: Math.round(timings.operations_count / (timings.total / 1000)),
                        avg_time_per_op: timings.operations_count > 0 ? Math.round(timings.total / timings.operations_count * 100) / 100 : 0,
                        batch_efficiency: timings.batch_success ? 'HIGH' : 'DEGRADED',
                        timeout_threshold: `${Block.TRANSACTION_TIMEOUT}ms`,
                        timeout_utilization: `${Math.round((timings.batch_transaction / Block.TRANSACTION_TIMEOUT) * 100)}%`,
                    }
                })
            }

            // Check if there are more operations to process immediately
            try {
                const remainingCount = await Block.redis?.llen(operationQueueKey)
                if (remainingCount && remainingCount > 0) {
                    if (Block.debug) {
                        console.log(`[Sharded] ${remainingCount} more operations remaining for block ${blockId}, will process in next cycle`)
                    }

                    // Trigger immediate processing for remaining operations by adding a job
                    const queue = Block.blockQueues.get(blockId)
                    if (queue) {
                        try {
                            await queue.add(`${blockId}_batch_processor`, { blockId }, {
                                delay: 1, // Minimal delay for faster processing
                                jobId: `${blockId}_immediate_${Date.now()}` // Unique job ID to prevent duplicates
                            })
                        } catch (queueErr) {
                            console.error(`[Sharded] Failed to schedule immediate processing for ${blockId}:`, queueErr)
                            // Don't stop the worker - this is not critical
                        }
                    }
                }
            } catch (redisErr) {
                console.error(`[Sharded] Failed to check remaining operations for ${blockId}:`, redisErr)
                // Don't stop the worker - this is not critical
            }

        } catch (err) {
            const totalTime = Date.now() - startTime
            console.error(`[Sharded] Error in batch sync for block ${blockId} after ${totalTime}ms:`, err)
            // Don't re-throw to avoid failing the recurring job
        }
    }

    static async queue_operation(
        blockId: string,
        {
            operation,
            model,
            args,
        }: { operation: string; model: string; args: any },
    ) {
        if (!Block.redis) {
            if (Block.debug) {
                console.log(`[Sharded] Redis not available for block ${blockId}`)
            }
            return // Skip this operation if Redis is not available
        }

        try {
            // Queue individual operation in Redis list
            const operationQueueKey = `block_operations:${blockId}`
            const operationData = JSON.stringify({
                operation,
                model,
                args,
                queued_at: Date.now(),
            })

            // Add to the end of the list (FIFO order)
            await Block.redis.rpush(operationQueueKey, operationData)

            if (Block.debug) {
                console.log('🔄 Queued operation in Redis:', { operation, model, args })
            }

            // Trigger immediate batch processing if this is a new operation queue
            // or if there might be a delay in processing
            try {
                const queue = Block.blockQueues.get(blockId)
                if (queue) {
                    // Check if there's already a job waiting/processing
                    const waiting = await queue.getWaiting()
                    const active = await queue.getActive()

                    // If no jobs are pending, trigger immediate processing
                    if (waiting.length === 0 && active.length === 0) {
                        await queue.add(`${blockId}_batch_processor`, { blockId }, {
                            delay: 1, // Minimal delay for immediate processing
                            jobId: `${blockId}_trigger_${Date.now()}` // Unique job ID
                        })

                        if (Block.debug) {
                            console.log(`🚀 Triggered immediate batch processing for ${blockId}`)
                        }
                    }
                }
            } catch (triggerErr) {
                // Don't fail the operation if triggering fails
                if (Block.debug) {
                    console.log(`[Sharded] Failed to trigger immediate processing for ${blockId}:`, triggerErr)
                }
            }
        } catch (err) {
            console.error('Error queueing operation in Redis:', err)
            throw err
        }
    }

    /**
     * Get failed operations from the dead letter queue
     */
    static async getFailedOperations(blockId: string): Promise<any[]> {
        if (!Block.redis) return []

        const deadLetterKey = `${Block.FAILED_OPERATIONS_KEY_PREFIX}:${blockId}`
        const failedOps = await Block.redis.lrange(deadLetterKey, 0, -1)

        return failedOps.map(op => {
            try {
                return JSON.parse(op)
            } catch (err) {
                console.error('[Sharded] Failed to parse failed operation:', op, err)
                return null
            }
        }).filter(op => op !== null)
    }

    /**
     * Clear failed operations from the dead letter queue
     */
    static async clearFailedOperations(blockId: string): Promise<number> {
        if (!Block.redis) return 0

        const deadLetterKey = `${Block.FAILED_OPERATIONS_KEY_PREFIX}:${blockId}`
        const count = await Block.redis.llen(deadLetterKey)
        await Block.redis.del(deadLetterKey)

        return count
    }

    /**
     * Retry failed operations (move them back to the main queue)
     */
    static async retryFailedOperations(blockId: string, maxOperations?: number): Promise<number> {
        if (!Block.redis) return 0

        const deadLetterKey = `${Block.FAILED_OPERATIONS_KEY_PREFIX}:${blockId}`
        const operationQueueKey = `block_operations:${blockId}`

        const operations = maxOperations
            ? await Block.redis.lrange(deadLetterKey, 0, maxOperations - 1)
            : await Block.redis.lrange(deadLetterKey, 0, -1)

        if (operations.length === 0) return 0

        const multi = Block.redis.multi()

        // Remove operations from dead letter queue
        if (maxOperations) {
            multi.ltrim(deadLetterKey, operations.length, -1)
        } else {
            multi.del(deadLetterKey)
        }

        // Add operations back to main queue (reset retry count)
        for (let i = operations.length - 1; i >= 0; i--) {
            try {
                const op = JSON.parse(operations[i])
                delete op.retry_count
                delete op.last_error
                delete op.final_error
                delete op.failed_at
                delete op.failed_permanently_at
                multi.lpush(operationQueueKey, JSON.stringify(op))
            } catch (err) {
                console.error('[Sharded] Failed to parse operation for retry:', operations[i], err)
            }
        }

        await multi.exec()

        if (Block.debug) {
            console.log(`[Sharded] Retried ${operations.length} failed operations for block ${blockId}`)
        }

        return operations.length
    }

    /**
     * Gracefully wait for all pending operations to complete before shutdown
     */
    static async gracefulShutdown(timeoutMs: number = 10000): Promise<void> {
        console.log('[Sharded] Starting graceful shutdown...')

        // Stop WAL maintenance first
        Block.stopWalMaintenance()

        // Perform final WAL checkpoints for all blocks
        try {
            console.log('[Sharded] Performing final WAL checkpoints...')
            const blockIds = Array.from(Block.blockClients.keys())
            await Promise.all(blockIds.map(blockId =>
                Block.forceWalCheckpoint(blockId, 'FULL').catch(err => {
                    console.error(`[Sharded] Final checkpoint failed for ${blockId}:`, err)
                })
            ))
        } catch (err) {
            console.error('[Sharded] Error during final WAL checkpoints:', err)
        }

        const startTime = Date.now()
        const blockIds = Array.from(Block.blockQueues.keys())

        while (Date.now() - startTime < timeoutMs) {
            let totalPending = 0

            // Check all blocks for pending operations
            for (const blockId of blockIds) {
                if (Block.redis) {
                    const pendingCount = await Block.redis.llen(`block_operations:${blockId}`)
                    totalPending += pendingCount
                }
            }

            if (totalPending === 0) {
                console.log('[Sharded] All operations completed, shutdown ready')
                return
            }

            console.log(`[Sharded] Waiting for ${totalPending} pending operations...`)
            await new Promise(resolve => setTimeout(resolve, 500))
        }

        console.warn(`[Sharded] Shutdown timeout reached, ${Date.now() - startTime}ms elapsed`)
    }

    /**
     * Force flush all pending batches for graceful shutdown
     */
    static async flushAllPendingBatches(): Promise<void> {
        const blockIds = Array.from(Block.blockQueues.keys())

        for (const blockId of blockIds) {
            // Trigger immediate batch processing
            const queue = Block.blockQueues.get(blockId)
            if (queue) {
                try {
                    await queue.add(`${blockId}_batch_processor`, { blockId }, {
                        delay: 0,
                        jobId: `${blockId}_shutdown_flush_${Date.now()}`
                    })
                } catch (err) {
                    console.error(`[Sharded] Error flushing batch for ${blockId}:`, err)
                }
            }
        }

        console.log('[Sharded] Triggered immediate batch processing for all blocks')
    }


    /**
     * Get orphaned queue information (blocks with items but no workers)
     */
    static async getOrphanedQueues(): Promise<Array<{ blockId: string, queueLength: number, hasWorker: boolean, hasQueue: boolean }>> {
        // Initialize Redis connection if not already done
        if (!Block.redis) {
            Block.redis = new Redis({
                host: process.env.REDIS_HOST ?? 'localhost',
                port: process.env.REDIS_PORT ? parseInt(process.env.REDIS_PORT) : 6379,
                password: process.env.REDIS_PASSWORD,
                retryStrategy: function (times: number) {
                    return Math.max(Math.min(Math.exp(times), 20000), 1000);
                },
                enableOfflineQueue: false,
            })
        }

        const orphanedQueues: Array<{ blockId: string, queueLength: number, hasWorker: boolean, hasQueue: boolean }> = []

        try {
            const operationKeys = await Block.redis.keys('block_operations:*')

            for (const key of operationKeys) {
                const blockId = key.replace('block_operations:', '')
                const queueLength = await Block.redis.llen(key)

                if (queueLength > 0) {
                    const hasWorker = Block.blockWorkers.has(blockId)
                    const hasQueue = Block.blockQueues.has(blockId)

                    if (!hasWorker || !hasQueue) {
                        orphanedQueues.push({
                            blockId,
                            queueLength,
                            hasWorker,
                            hasQueue
                        })
                    }
                }
            }
        } catch (error) {
            console.error('[Sharded] Error getting orphaned queues:', error)
            throw error
        }

        return orphanedQueues
    }

    /**
     * Get diagnostic information about the sync workers and queues
     */
    static async getDiagnostics(): Promise<any> {
        const diagnostics: any = {
            redis_status: Block.redis?.status,
            active_blocks: [],
            worker_status: {},
            queue_status: {},
        }

        for (const [blockId, queue] of Block.blockQueues.entries()) {
            diagnostics.active_blocks.push(blockId)

            // Worker status
            const worker = Block.blockWorkers.get(blockId)
            diagnostics.worker_status[blockId] = {
                exists: !!worker,
                is_running: worker ? !worker.isRunning() : false,
            }

            // Queue status
            try {
                const waiting = await queue.getWaiting()
                const active = await queue.getActive()
                const failed = await queue.getFailed()

                diagnostics.queue_status[blockId] = {
                    waiting: waiting.length,
                    active: active.length,
                    failed: failed.length,
                }
            } catch (err) {
                diagnostics.queue_status[blockId] = {
                    error: err instanceof Error ? err.message : String(err)
                }
            }

            // Redis queue length
            if (Block.redis) {
                try {
                    const operationQueueKey = `block_operations:${blockId}`
                    const pendingCount = await Block.redis.llen(operationQueueKey)
                    diagnostics.queue_status[blockId].redis_pending = pendingCount
                } catch (err) {
                    diagnostics.queue_status[blockId].redis_error = err instanceof Error ? err.message : String(err)
                }
            }
        }

        // Add orphaned queue information
        try {
            diagnostics.orphaned_queues = await Block.getOrphanedQueues()
        } catch (err) {
            diagnostics.orphaned_queues_error = err instanceof Error ? err.message : String(err)
        }

        // Add recovery worker information
        diagnostics.ongoing_recovery_workers = Array.from(Block.ongoingRecoveryWorkers)
        diagnostics.recovery_client_count = Block.recoveryMainClients.size

        return diagnostics
    }

    static async add_hooks(
        client: Prisma.DefaultPrismaClient,
        blockId: string,
    ) {
        const mainClient = await Block.mainClients.get(blockId)
        if (!mainClient) {
            throw new Error(`Main client for ${blockId} not found`)
        }

        // Add a write hook to the client, to sync all write operations for all models and add them to a queue
        const newClient = client.$extends({
            query: {
                $allModels: {
                    async create({ operation, args, model, query, ...rest }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)

                        Block.updateLastSeenThrottled(blockId)

                        if (Block.debug) {
                            console.log('🔄 create', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }
                        // Execute in block DB first to get the ID
                        const result = await query(args)

                        // Queue the operation with the result's ID
                        await Block.queue_operation(blockId, {
                            operation,
                            model,
                            args: {
                                ...args,
                                data: {
                                    ...args.data,
                                    id: result.id,
                                },
                            },
                        })
                        return result
                    },

                    async createMany({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)

                        Block.updateLastSeenThrottled(blockId)

                        if (Block.debug) {
                            console.log('🔄 createMany', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }
                        // tranform and include the ids
                        const data = Array.isArray(args.data)
                            ? args.data.map((item) => ({
                                ...item,
                                id: item.id ?? crypto.randomUUID(),
                            }))
                            : {
                                ...args.data,
                                id: args.data.id ?? crypto.randomUUID(),
                            }

                        args = { ...args, data: data as any }

                        const result = await query(args)
                        await Block.queue_operation(blockId, {
                            operation,
                            model,
                            args,
                        })
                        return result
                    },

                    async update({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)

                        if (Block.debug) {
                            console.log('🔄 update', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }


                        const result = await query(args)

                        await Block.queue_operation(blockId, {
                            operation,
                            model,
                            args,
                        })
                        Block.updateLastSeenThrottled(blockId)
                        return result
                    },

                    async updateMany({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)

                        if (Block.debug) {
                            console.log('🔄 updateMany', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }
                        const result = await query(args)


                        Block.updateLastSeenThrottled(blockId)
                        await Block.queue_operation(blockId, {
                            operation,
                            model,
                            args,
                        })
                        return result
                    },

                    async upsert({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)

                        if (Block.debug) {
                            console.log('🔄 upsert', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }
                        const result = await query(args)

                        Block.updateLastSeenThrottled(blockId)
                        await Block.queue_operation(blockId, {
                            operation,
                            model,
                            args,
                        })
                        return result

                    },

                    async delete({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)



                        if (Block.debug) {
                            console.log('🔄 delete', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }

                        // Execute the delete in block DB first
                        const result = await query(args)

                        Block.updateLastSeenThrottled(blockId)
                        // Queue the operation for main DB sync
                        await Block.queue_operation(blockId, {
                            operation,
                            model,
                            args,
                        })
                        return result

                    },

                    async deleteMany({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)

                        try {
                            if (Block.debug) {
                                console.log('🔄 deleteMany', {
                                    operation,
                                    args,
                                    model,
                                    query,
                                })
                            }
                            // Execute the delete in block DB first
                            const result = await query(args)

                            Block.updateLastSeenThrottled(blockId)
                            // Queue the operation for main DB sync
                            await Block.queue_operation(blockId, {
                                operation,
                                model,
                                args,
                            })
                            return result
                        } finally {

                        }
                    },

                    async findFirst({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)

                        if (Block.debug) {
                            console.log('🔄 findFirst', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }

                        // Execute query with automatic WAL optimization and fallback
                        const currentBlockClient = Block.blockClients.get(blockId)
                        if (!currentBlockClient) {
                            throw new Error(`Block client not found for ${blockId}`)
                        }

                        let result
                        try {
                            // Try with controlled transaction to prevent long-lived readers
                            result = await currentBlockClient.$transaction(async (tx: any) => {
                                return await (tx as any)[model.toLowerCase()][operation](args)
                            }, {
                                timeout: 30000, // Increased timeout for read operations under heavy load
                                maxWait: 10000  // Increased maxWait for heavy load
                            })
                        } catch (txErr: any) {
                            // If transaction fails, fallback to direct query (still better than failure)
                            if (Block.debug) {
                                console.log(`[Sharded] Transaction failed for ${model}.${operation}, using fallback:`, txErr.message)
                            }
                            result = await query(args)
                        }

                        Block.updateLastSeenThrottled(blockId)

                        // If no result found in block, check main database
                        if (!result) {
                            if (Block.debug) {
                                console.log(`[Sharded] No result found in block for ${model}.findFirst, checking main database`)
                            }

                            try {
                                const mainResult = await mainClient.$transaction(async (tx) => {
                                    return await (tx as any)[model.toLowerCase()][operation](args)
                                }, {
                                    timeout: 30000, // Increased timeout for main client fallback
                                    maxWait: 10000  // Increased maxWait for heavy load
                                })

                                if (mainResult) {
                                    if (Block.debug) {
                                        console.log(`[Sharded] Found result in main database for ${model}.findFirst, reloading block`)
                                    }
                                    // Reload block data since we found the record in main DB
                                    await Block.reloadBlockData(blockId)
                                    // Try the query again after reload with a fresh transaction
                                    result = await currentBlockClient.$transaction(async (tx: any) => {
                                        return await (tx as any)[model.toLowerCase()][operation](args)
                                    }, {
                                        timeout: 30000, // Increased timeout for block reload under heavy load
                                        maxWait: 10000  // Increased maxWait for heavy load
                                    })
                                }
                            } catch (mainErr) {
                                if (Block.debug) {
                                    console.log(`[Sharded] Main database query failed for ${model}.findFirst:`, mainErr)
                                }
                                // Continue with null result
                            }
                        }

                        return result
                    },

                    async findMany({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)


                        Block.updateLastSeenThrottled(blockId)
                        if (Block.debug) {
                            console.log('🔄 findMany', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }
                        const result = await query(args)
                        return result
                    },

                    async findUnique({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)


                        Block.updateLastSeenThrottled(blockId)

                        if (Block.debug) {
                            console.log('🔄 findUnique', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }
                        const result = await query(args)
                        return result
                    },

                    async findUniqueOrThrow({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)



                        if (Block.debug) {
                            console.log('🔄 findUniqueOrThrow', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }

                        const result = await query(args)

                        Block.updateLastSeenThrottled(blockId)
                        return result
                    },

                    async findFirstOrThrow({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)
                        if (Block.debug) {
                            console.log('🔄 findFirstOrThrow', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }

                        const result = await query(args)
                        Block.updateLastSeenThrottled(blockId)
                        return result
                    },

                    // these need to be pulled directly from the main client, Ideally you don't want to do this
                    async count({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)


                        Block.updateLastSeenThrottled(blockId)


                        if (Block.debug) {
                            console.log('🔄 count', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }
                        const result = await query(args)
                        return result


                    },

                    async aggregate({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)


                        Block.updateLastSeenThrottled(blockId)


                        if (Block.debug) {
                            console.log('🔄 aggregate', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }
                        const result = await query(args)
                        return result


                    },

                    async groupBy({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)
                        Block.updateLastSeenThrottled(blockId)

                        if (Block.debug) {
                            console.log('🔄 groupBy', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }
                        const result = await query(args)
                        return result
                    },
                },
            },
        })

        return newClient as typeof client & Prisma.DefaultPrismaClient
    }

    static async invalidate(blockId: string): Promise<void> {
        if (Block.debug) {
            console.log('[Sharded] Starting invalidation of block:', blockId)
        }

        // Prevent double invalidation locally first
        if (Block.invalidatingBlocks.has(blockId)) {
            if (Block.debug) {
                console.log('[Sharded] Block already being invalidated locally:', blockId)
            }
            return
        }

        Block.invalidatingBlocks.add(blockId)

        try {
            // First, wait for ongoing operations to complete WITHOUT holding any locks
            // This allows sync operations to finish their work naturally
            const maxWaitTime = 3600000 // 60 minutes timeout for operations to complete

            if (Block.debug) {
                console.log(`[Sharded] Waiting for ongoing operations to complete before acquiring lock for block ${blockId}`)
            }

            // Wait for write operations to complete (reads can continue and will fail gracefully)
            const startTime = Date.now()
            let lastLogTime = 0
            while (Block.activeWriteOperations.has(blockId)) {
                const elapsed = Date.now() - startTime
                if (elapsed > maxWaitTime) {
                    console.warn(`[Sharded] Timeout waiting for write operations to complete for block ${blockId}, proceeding with invalidation`)
                    break
                }
                // Log every 2 seconds instead of every 100ms to reduce noise
                if (Block.debug && elapsed - lastLogTime > 2000) {
                    console.log(`[Sharded] Waiting for ${Block.activeWriteOperations.get(blockId)} active write operations to complete for block ${blockId} (${elapsed}ms elapsed)`)
                    lastLogTime = elapsed
                }
                await new Promise(resolve => setTimeout(resolve, 100))
            }

            // Wait for pending sync operations to complete
            if (Block.debug) {
                console.log(`[Sharded] Waiting for pending syncs before acquiring lock for block ${blockId}`)
            }
            await Block.waitForPendingSyncs(blockId, maxWaitTime)

            // NOW acquire the block lock after operations have completed
            const blockLockKey = `block_lock:${blockId}`
            const processId = `${process.pid}_${Date.now()}`

            let lockAcquired = false
            const lockWaitTime = 30000 // 30 seconds for lock acquisition
            const lockStartTime = Date.now()

            while (!lockAcquired && (Date.now() - lockStartTime < lockWaitTime)) {
                const result = await Block.redis?.set(blockLockKey, processId, 'PX', 30000, 'NX')
                if (result === 'OK') {
                    lockAcquired = true
                    if (Block.debug) {
                        console.log(`[Sharded] Acquired block lock for invalidation: ${blockId}`)
                    }
                } else {
                    if (Block.debug) {
                        console.log(`[Sharded] Block lock exists for ${blockId}, waiting...`)
                    }
                    await new Promise(resolve => setTimeout(resolve, 100))
                }
            }

            if (!lockAcquired) {
                if (Block.debug) {
                    console.log(`[Sharded] Failed to acquire block lock for invalidation of ${blockId}`)
                }
                return // Another process is using the block
            }

            // Final check for any new operations that might have started while waiting for lock
            // This ensures we don't invalidate while new operations are in progress
            if (Block.activeWriteOperations.has(blockId)) {
                if (Block.debug) {
                    console.log(`[Sharded] New write operations detected for ${blockId}, releasing lock and retrying`)
                }
                await Block.redis?.del(blockLockKey)
                Block.invalidatingBlocks.delete(blockId)
                // Retry invalidation
                return Block.invalidate(blockId)
            }

            // Double-check pending syncs
            const pendingCount = await Block.redis?.llen(`block_operations:${blockId}`) || 0
            if (pendingCount > 0) {
                if (Block.debug) {
                    console.log(`[Sharded] New sync operations detected for ${blockId} (${pendingCount} pending), releasing lock and retrying`)
                }
                await Block.redis?.del(blockLockKey)
                Block.invalidatingBlocks.delete(blockId)
                // Retry invalidation
                return Block.invalidate(blockId)
            }

            // Clean up Redis operation queue
            const operationQueueKey = `block_operations:${blockId}`
            await Block.redis?.del(operationQueueKey)

            // Get references to resources before removing from maps
            const queue = Block.blockQueues.get(blockId)
            const worker = Block.blockWorkers.get(blockId)

            // Remove from maps atomically
            Block.blockClients.delete(blockId)
            Block.blockClientsWithHooks.delete(blockId)
            Block.mainClients.delete(blockId)
            Block.blockQueues.delete(blockId)
            Block.blockWorkers.delete(blockId)
            Block.blockLoaders.delete(blockId)

            // Close resources
            if (worker) {
                if (Block.debug) {
                    console.log('[Sharded] Closing worker:', blockId)
                }
                await worker.close()
            }

            // Close queue if it exists
            if (queue) {
                try {
                    await queue.close()
                } catch (err) {
                    if (Block.debug) {
                        console.error('[Sharded] Error closing queue:', err)
                    }
                }
            }

            // Delete files last
            await Block.delete_block(blockId)

            if (Block.debug) {
                console.log('[Sharded] Successfully invalidated block:', blockId)
            }

        } catch (error) {
            console.error('[Sharded] Error during block invalidation:', error)
            // Even if invalidation fails, we should remove from invalidating set
        } finally {
            Block.invalidatingBlocks.delete(blockId)

            // Only release the Redis block lock if we might have acquired it
            // (This handles cases where we fail before acquiring the lock)
            const blockLockKey = `block_lock:${blockId}`
            const lockValue = await Block.redis?.get(blockLockKey)
            if (lockValue && lockValue.startsWith(`${process.pid}_`)) {
                await Block.redis?.del(blockLockKey)
                if (Block.debug) {
                    console.log(`[Sharded] Released block lock after invalidation: ${blockId}`)
                }
            } else if (Block.debug && lockValue) {
                console.log(`[Sharded] Block lock for ${blockId} owned by different process, not releasing`)
            }
        }
    }

    /**
     * Check for orphaned queues (blocks with operations but no active workers)
     * and create ephemeral recovery workers that process all operations until done
     */
    static async checkOrphanedQueues<T>(mainClient: T | Prisma.DefaultPrismaClient, connection?: ConnectionOptions): Promise<void> {
        // Initialize Redis connection if not already done
        if (!Block.redis) {
            Block.redis = new Redis({
                host: process.env.REDIS_HOST ?? 'localhost',
                port: process.env.REDIS_PORT ? parseInt(process.env.REDIS_PORT) : 6379,
                password: process.env.REDIS_PASSWORD,
                retryStrategy: function (times: number) {
                    return Math.max(Math.min(Math.exp(times), 20000), 1000);
                },
                enableOfflineQueue: false,
            })
        }

        try {
            // Get all operation queue keys
            const operationKeys = await Block.redis.keys('block_operations:*')

            for (const key of operationKeys) {
                const blockId = key.replace('block_operations:', '')

                // Check if the queue has items
                const queueLength = await Block.redis.llen(key)
                if (queueLength === 0) {
                    continue // No items in queue
                }

                // Check if we have an active worker for this block
                const hasWorker = Block.blockWorkers.has(blockId)
                const hasQueue = Block.blockQueues.has(blockId)

                if (!hasWorker || !hasQueue) {
                    // Check if recovery is already ongoing for this block
                    if (Block.ongoingRecoveryWorkers.has(blockId)) {
                        if (Block.debug) {
                            console.log(`[Sharded] Recovery already ongoing for block ${blockId}, skipping`)
                        }
                        continue
                    }

                    console.log(`[Sharded] Found orphaned queue for block ${blockId} with ${queueLength} items, creating ephemeral recovery worker`)

                    try {
                        if (!mainClient) {
                            console.warn(`[Sharded] Cannot create recovery worker for block ${blockId}: no main client provided`)
                            continue
                        }

                        // Create ephemeral recovery worker that processes until queue is empty
                        await Block.createEphemeralRecoveryWorker(blockId, mainClient, connection)

                    } catch (restartError) {
                        console.error(`[Sharded] Failed to create recovery worker for orphaned queue ${blockId}:`, restartError)
                    }
                }
            }
        } catch (error) {
            console.error('[Sharded] Error checking orphaned queues:', error)
        }
    }

    /**
     * Create an ephemeral recovery worker that processes all operations until the queue is empty
     * and then automatically closes itself
     */
    static async createEphemeralRecoveryWorker(
        blockId: string,
        mainClient: any,
        connection?: ConnectionOptions
    ): Promise<void> {
        // Initialize Redis connection if not already done
        if (!Block.redis) {
            Block.redis = new Redis({
                host: process.env.REDIS_HOST ?? 'localhost',
                port: process.env.REDIS_PORT ? parseInt(process.env.REDIS_PORT) : 6379,
                password: process.env.REDIS_PASSWORD,
                retryStrategy: function (times: number) {
                    return Math.max(Math.min(Math.exp(times), 20000), 1000);
                },
                enableOfflineQueue: false,
            })
        }

        // Mark recovery as ongoing
        Block.ongoingRecoveryWorkers.add(blockId)
        console.log(`[Sharded] Creating ephemeral recovery worker for block ${blockId}`)

        // Add the provided client to recovery clients map so batch_sync_operation can find it
        Block.recoveryMainClients.set(blockId, mainClient)

        // Create a unique queue name for this recovery worker
        const recoveryQueueName = `${blockId}_recovery_${Date.now()}`

        // Create the recovery queue
        const recoveryQueue = new Queue(recoveryQueueName, {
            connection: connection ?? {
                host: process.env.REDIS_HOST ?? 'localhost',
                port: process.env.REDIS_PORT ? parseInt(process.env.REDIS_PORT) : 6379,
                password: process.env.REDIS_PASSWORD,
                retryStrategy: function (times: number) {
                    return Math.max(Math.min(Math.exp(times), 20000), 1000);
                },
                maxRetriesPerRequest: null,
                enableOfflineQueue: true,
            },
        })

        // Create worker that reuses batch_sync_operation and processes until empty
        const recoveryWorker = new Worker(recoveryQueueName, async (job: Job<{ blockId: string }>) => {
            const startTime = Date.now()
            let totalBatches = 0

            try {
                console.log(`[Sharded] Starting ephemeral recovery processing for block ${blockId}`)

                // Keep processing until the queue is empty
                while (true) {
                    // Check if there are any operations left
                    const operationQueueKey = `block_operations:${blockId}`
                    const queueLength = await Block.redis!.llen(operationQueueKey)

                    if (queueLength === 0) {
                        console.log(`[Sharded] Ephemeral recovery completed for block ${blockId}. Processed ${totalBatches} batches in ${Date.now() - startTime}ms`)
                        break
                    }

                    // Reuse the existing batch_sync_operation logic
                    await Block.batch_sync_operation(job)
                    totalBatches++

                    if (Block.debug) {
                        console.log(`[Sharded] Ephemeral recovery batch ${totalBatches} completed for ${blockId}`)
                    }
                }

            } catch (error) {
                console.error(`[Sharded] Error in ephemeral recovery worker for ${blockId}:`, error)
                throw error // Re-throw to mark job as failed
            } finally {
                // Clean up regardless of success or failure
                setTimeout(async () => {
                    try {
                        // Mark recovery as complete
                        Block.ongoingRecoveryWorkers.delete(blockId)

                        // Remove from recovery clients map
                        Block.recoveryMainClients.delete(blockId)

                        // Close worker and queue
                        await recoveryWorker.close()
                        await recoveryQueue.close()

                        console.log(`[Sharded] Ephemeral recovery worker cleaned up for block ${blockId}`)
                    } catch (cleanupError) {
                        console.error(`[Sharded] Error during cleanup for ${blockId}:`, cleanupError)
                    }
                }, 100) // Small delay to ensure job completes before cleanup
            }
        }, {
            concurrency: 1,
            connection: connection ?? {
                host: process.env.REDIS_HOST ?? 'localhost',
                port: process.env.REDIS_PORT ? parseInt(process.env.REDIS_PORT) : 6379,
                password: process.env.REDIS_PASSWORD,
                retryStrategy: function (times: number) {
                    return Math.max(Math.min(Math.exp(times), 20000), 1000);
                },
                maxRetriesPerRequest: null,
                enableOfflineQueue: true,
            },
        })

        recoveryWorker.on("error", (error) => {
            console.error(`[Sharded] Error in ephemeral recovery worker for ${blockId}:`, error);
        });

        await recoveryWorker.waitUntilReady()

        // Add a single job to start the recovery process
        await recoveryQueue.add(`${blockId}_recovery`, { blockId }, {
            removeOnComplete: true,
            removeOnFail: false,
        })
    }


    /**
     * invalidation worker process
     *  - Will watch every block created on this node/machine
     *  - If block hasn't been accessed or written to longer than the ttl, configured on the master node
     *  - It will run this check every 10 seconds (override with interval) on all blocks
     *  - Also checks for orphaned queues (blocks with items but no workers) every 30 seconds
     * 
     * @param options
     * Watcher options:
     *  - ttl: in seconds, The default ttl for all blocks, if not set on the master node
     *  - interval: in milliseconds, The interval to check all blocks
     *  - orphanedQueueCheckInterval: in milliseconds, The interval to check for orphaned queues
     *  - mainClient: Main Prisma client for orphaned queue recovery (important after restarts)
     *  - connection: The connection options for the queue
     *  */

    static async watch<T>(options: WatchOptions<T>) {
        const invalidationQueue = new Queue('invalidation', {
            connection: options.connection ?? {
                host: process.env.REDIS_HOST ?? 'localhost',
                port: process.env.REDIS_PORT
                    ? parseInt(process.env.REDIS_PORT)
                    : 6379,
                password: process.env.REDIS_PASSWORD,
                retryStrategy: function (times: number) {
                    return Math.max(Math.min(Math.exp(times), 20000), 1000);
                },
                enableOfflineQueue: false,
            },
        })

        const orphanedQueue = new Queue('orphaned', {
            connection: options.connection ?? {
                host: process.env.REDIS_HOST ?? 'localhost',
                port: process.env.REDIS_PORT
                    ? parseInt(process.env.REDIS_PORT)
                    : 6379,
                password: process.env.REDIS_PASSWORD,
                retryStrategy: function (times: number) {
                    return Math.max(Math.min(Math.exp(times), 20000), 1000);
                },
                enableOfflineQueue: false,
            },
        })

        invalidationQueue.on("error", (error) => {
            console.error(`[sharded][watch] Unhandled error block invalidation queue: ${invalidationQueue.name}`, error);
        });

        orphanedQueue.on("error", (error) => {
            console.error(`[sharded][watch] Unhandled error block orphaned queue: ${orphanedQueue.name}`, error);
        });

        await invalidationQueue.waitUntilReady()
        await orphanedQueue.waitUntilReady()

        await invalidationQueue.upsertJobScheduler("check-invalidation-every", {
            every: options.interval ?? 10000,
        }, {
            name: "check-invalidation-every",
            data: {
                ttl: options.ttl ?? 60,
            },
        })

        // Add scheduler for orphaned queue checks
        await orphanedQueue.upsertJobScheduler("check-orphaned-queues-every", {
            every: options.orphanedQueueCheckInterval ?? 30000,
        }, {
            name: "check-orphaned-queues-every",
            data: {},
        })


        new Worker(invalidationQueue.name, async (job: Job<{ ttl?: number }>) => {
            if (Block.debug) {
                console.log('[Sharded] checking invalidation')
            }
            // Handle TTL-based invalidation
            const lastSeen = await Block.redis?.hgetall("last_seen")
            for (const blockId in lastSeen) {
                const lastSeenTime = parseInt(lastSeen[blockId])
                const blockTtl = parseInt((await Block.redis?.hget("block_ttl", blockId)) ?? `${job.data.ttl ?? 60}`) * 1000
                if (Date.now() - lastSeenTime > blockTtl) {
                    // Check if block has active operations, creation, or reload before invalidating
                    if (await Block.canInvalidate(blockId)) {
                        await Block.invalidate(blockId)
                        Block.redis?.hdel("last_seen", blockId)
                        Block.redis?.hdel("block_ttl", blockId)
                        console.log('[Sharded] Invalidated block:', blockId)
                    } else {
                        // Extend grace period for active blocks
                        const graceTime = Date.now() - (blockTtl * 0.8)
                        Block.redis?.hset("last_seen", blockId, graceTime)
                        if (Block.debug) {
                            console.log(`[Sharded] Extended grace period for active/locked block: ${blockId}`)
                        }
                    }
                }
            }
        }, {
            connection: options.connection ?? {
                host: process.env.REDIS_HOST ?? 'localhost',
                port: process.env.REDIS_PORT
                    ? parseInt(process.env.REDIS_PORT)
                    : 6379,
                password: process.env.REDIS_PASSWORD,
                retryStrategy: function (times: number) {
                    return Math.max(Math.min(Math.exp(times), 20000), 1000);
                },
                maxRetriesPerRequest: null,
                enableOfflineQueue: true,
            },
        })

        new Worker(orphanedQueue.name, async (job: Job<{ ttl?: number }>) => {
            if (Block.debug) {
                console.log('[Sharded] Checking for orphaned queues')
            }
            await Block.checkOrphanedQueues(options.mainClient, options.connection)
        }, {
            connection: options.connection ?? {
                host: process.env.REDIS_HOST ?? 'localhost',
                port: process.env.REDIS_PORT
                    ? parseInt(process.env.REDIS_PORT)
                    : 6379,
                password: process.env.REDIS_PASSWORD,
            },
        })
    }
}
