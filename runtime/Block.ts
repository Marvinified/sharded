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


interface WatchOptions {
    ttl?: number
    interval?: number
    connection?: ConnectionOptions
}

export class Block {
    private static blockClientsWithHooks = new Map<
        string,
        Prisma.DefaultPrismaClient
    >()
    public static blockClients = new Map<string, Prisma.DefaultPrismaClient>()
    private static mainClients = new Map<string, Prisma.DefaultPrismaClient>()
    private static blockQueues = new Map<string, Queue>()
    private static blockWorkers = new Map<string, Worker>()
    private static locks = new Map<string, Promise<void>>()
    private static redis: Redis | null = null
    private static debug = false
    // Reference counting for active operations
    private static activeOperations = new Map<string, number>()
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
    // Batch processing limits
    private static MAX_BATCH_SIZE = 50 // Maximum operations per batch
    private static TRANSACTION_TIMEOUT = 30000 // 30 seconds transaction timeout
    // Error handling
    private static MAX_OPERATION_RETRIES = 3 // Maximum retries for failed operations
    private static FAILED_OPERATIONS_KEY_PREFIX = 'failed_operations' // Redis key prefix for failed operations

    static setDebug(debug: boolean) {
        Block.debug = debug
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

            const blockClient = new BlockPrismaClient({
                datasources: {
                    db: {
                        url: `file:${blockPath}?journal_mode=WAL`,
                    },
                },
                ...config.prismaOptions,
            })

            // Enable WAL mode and set pragmas optimized for multi-process performance
            try {
                await blockClient.$queryRaw`PRAGMA journal_mode=WAL`
                await blockClient.$queryRaw`PRAGMA synchronous=NORMAL`
                await blockClient.$queryRaw`PRAGMA busy_timeout=5000`
                await blockClient.$queryRaw`PRAGMA cache_size=-2000` // Use 2MB of memory for cache
                await blockClient.$queryRaw`PRAGMA wal_autocheckpoint=1000` // Checkpoint every 1000 pages
                await blockClient.$queryRaw`PRAGMA mmap_size=268435456` // 256MB memory mapping for better I/O
                await blockClient.$queryRaw`PRAGMA temp_store=MEMORY` // Keep temp tables in memory
            } catch (err) {
                if (Block.debug) {
                    console.error(
                        '[Sharded] Error setting SQLite pragmas:',
                        err,
                    )
                }
            }

            // Test query to confirm which file is being used
            try {
                const tables = await blockClient.$queryRawUnsafe(
                    'SELECT name FROM sqlite_master WHERE type="table";',
                )
                if (Block.debug) {
                    console.log(
                        '[Sharded] Tables in block DB',
                        blockPath,
                        ':',
                        tables,
                    )
                }
            } catch (err) {
                if (Block.debug) {
                    console.error(
                        '[Sharded] Error querying block DB',
                        blockPath,
                        ':',
                        err,
                    )
                }
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
                console.log(`[Sharded] Waiting for ${pendingCount} pending syncs for block ${blockId}`)
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
            // Wait for any active operations to complete before reloading
            const maxWaitTime = 30000 // 30 seconds max wait
            const startTime = Date.now()
            while (Block.activeOperations.has(blockId)) {
                if (Date.now() - startTime > maxWaitTime) {
                    console.warn(`[Sharded] Timeout waiting for operations to complete during reload of block ${blockId}, proceeding anyway`)
                    break
                }
                if (Block.debug) {
                    console.log(`[Sharded] Waiting for ${Block.activeOperations.get(blockId)} active operations to complete before reloading block ${blockId}`)
                }
                await new Promise(resolve => setTimeout(resolve, 100))
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
     * Increment the operation count for a block
     */
    static incrementOperationCount(blockId: string) {
        const current = Block.activeOperations.get(blockId) || 0
        Block.activeOperations.set(blockId, current + 1)
        if (Block.debug) {
            console.log(`[Sharded] Operation count for ${blockId}: ${current + 1}`)
        }
    }

    /**
     * Decrement the operation count for a block
     */
    static decrementOperationCount(blockId: string) {
        const current = Block.activeOperations.get(blockId) || 0
        if (current > 1) {
            Block.activeOperations.set(blockId, current - 1)
        } else {
            Block.activeOperations.delete(blockId)
        }
        if (Block.debug) {
            console.log(`[Sharded] Operation count for ${blockId}: ${Math.max(0, current - 1)}`)
        }
    }

    /**
     * Check if a block can be safely invalidated (no active operations, creation, or reload)
     */
    static async canInvalidate(blockId: string): Promise<boolean> {
        // Check for active operations and double invalidation
        if (Block.activeOperations.has(blockId) || Block.invalidatingBlocks.has(blockId)) {
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
            const mainClient = Block.mainClients.get(blockId)
            if (!mainClient) {
                if (Block.debug) {
                    console.log(`[Sharded] Main client for block ${blockId} not found - block may have been invalidated`)
                }
                return
            }

            // Atomically get and remove operations to prevent race conditions
            const operationQueueKey = `block_operations:${blockId}`

            // Use MULTI/EXEC to atomically get operations and clear the list
            // Limit batch size to prevent transaction timeouts
            const redisStartTime = Date.now()
            const multi = Block.redis?.multi()
            if (!multi) return

            // Get limited number of operations and remove them atomically
            multi.lrange(operationQueueKey, 0, Block.MAX_BATCH_SIZE - 1)
            multi.ltrim(operationQueueKey, Block.MAX_BATCH_SIZE, -1)
            const results = await multi.exec()
            timings.redis_fetch = Date.now() - redisStartTime

            if (!results || !results[0] || !results[0][1]) {
                return // No operations to process
            }

            const operations = results[0][1] as string[]
            if (operations.length === 0) {
                return
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
                    maxWait: 5000,
                })
                
                batchSuccess = true
                timings.batch_transaction = Date.now() - batchStartTime
                timings.batch_success = true
                
                if (Block.debug) {
                    console.log(`[Sharded] Batch transaction succeeded for ${parsedOperations.length} operations in ${timings.batch_transaction}ms`)
                }
                
            } catch (err: any) {
                timings.batch_transaction = Date.now() - batchStartTime
                
                // Batch failed - need to isolate the problematic operation(s)
                if (Block.debug) {
                    console.log(`[Sharded] Batch transaction failed after ${timings.batch_transaction}ms, isolating operations: ${err.message}`)
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
                            timeout: 10000,
                            maxWait: 2000,
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

            // Log comprehensive timing information
            console.log(`[Sharded] Batch sync completed for ${blockId}:`, {
                total_time: `${timings.total}ms`,
                operations_processed: timings.operations_count,
                batch_success: timings.batch_success,
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
                }
            })

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
                                delay: 10, // Small delay to prevent overwhelming
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

                        Block.incrementOperationCount(blockId)
                        Block.updateLastSeenThrottled(blockId)

                        try {
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
                        } finally {
                            Block.decrementOperationCount(blockId)
                        }
                    },

                    async createMany({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)

                        Block.incrementOperationCount(blockId)
                        Block.updateLastSeenThrottled(blockId)

                        try {
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
                        } finally {
                            Block.decrementOperationCount(blockId)
                        }
                    },

                    async update({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)

                        try {
                            if (Block.debug) {
                                console.log('🔄 update', {
                                    operation,
                                    args,
                                    model,
                                    query,
                                })
                            }
                            
                            try {
                                const result = await query(args)

                                await Block.queue_operation(blockId, {
                                    operation,
                                    model,
                                    args,
                                })
                                Block.incrementOperationCount(blockId)
                                Block.updateLastSeenThrottled(blockId)
                                return result
                            } catch (err: any) {
                                // Handle specific Prisma errors gracefully
                                if (err.code === 'P2025') {
                                    // Record not found for update - this might be a race condition
                                    console.warn(`[Sharded] Update failed - record not found in ${model}:`, {
                                        blockId,
                                        where: args.where,
                                        error: err.message
                                    })
                                    
                                    // Still queue the operation for sync (main DB might have it)
                                    await Block.queue_operation(blockId, {
                                        operation,
                                        model,
                                        args,
                                    })
                                    Block.incrementOperationCount(blockId)
                                    Block.updateLastSeenThrottled(blockId)
                                    
                                    // Re-throw the error to maintain API contract
                                    throw err
                                } else {
                                    // For other errors, don't queue and re-throw
                                    throw err
                                }
                            }
                        } finally {
                            Block.decrementOperationCount(blockId)
                        }
                    },

                    async updateMany({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)
                        try {
                            if (Block.debug) {
                                console.log('🔄 updateMany', {
                                    operation,
                                    args,
                                    model,
                                    query,
                                })
                            }
                            const result = await query(args)

                            Block.incrementOperationCount(blockId)
                            Block.updateLastSeenThrottled(blockId)
                            await Block.queue_operation(blockId, {
                                operation,
                                model,
                                args,
                            })
                            return result
                        } finally {
                            Block.decrementOperationCount(blockId)
                        }
                    },

                    async upsert({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)

                        

                        try {
                            if (Block.debug) {
                                console.log('🔄 upsert', {
                                    operation,
                                    args,
                                    model,
                                    query,
                                })
                            }
                            const result = await query(args)
                            Block.incrementOperationCount(blockId)
                            Block.updateLastSeenThrottled(blockId)
                            await Block.queue_operation(blockId, {
                                operation,
                                model,
                                args,
                            })
                            return result
                        } finally {
                            Block.decrementOperationCount(blockId)
                        }
                    },

                    async delete({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)

                        

                        try {
                            if (Block.debug) {
                                console.log('🔄 delete', {
                                    operation,
                                    args,
                                    model,
                                    query,
                                })
                            }
                            
                            try {
                                // Execute the delete in block DB first
                                const result = await query(args)
                                Block.incrementOperationCount(blockId)
                                Block.updateLastSeenThrottled(blockId)
                                // Queue the operation for main DB sync
                                await Block.queue_operation(blockId, {
                                    operation,
                                    model,
                                    args,
                                })
                                return result
                            } catch (err: any) {
                                // Handle specific Prisma errors gracefully
                                if (err.code === 'P2025') {
                                    // Record not found for delete - this might be a race condition
                                    console.warn(`[Sharded] Delete failed - record not found in ${model}:`, {
                                        blockId,
                                        where: args.where,
                                        error: err.message
                                    })
                                    
                                    // Still queue the operation for sync (main DB might have it)
                                    await Block.queue_operation(blockId, {
                                        operation,
                                        model,
                                        args,
                                    })
                                    Block.incrementOperationCount(blockId)
                                    Block.updateLastSeenThrottled(blockId)
                                    
                                    // Re-throw the error to maintain API contract
                                    throw err
                                } else {
                                    // For other errors, don't queue and re-throw
                                    throw err
                                }
                            }
                        } finally {
                            Block.decrementOperationCount(blockId)
                        }
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
                            Block.incrementOperationCount(blockId)
                            Block.updateLastSeenThrottled(blockId)
                            // Queue the operation for main DB sync
                            await Block.queue_operation(blockId, {
                                operation,
                                model,
                                args,
                            })
                            return result
                        } finally {
                            Block.decrementOperationCount(blockId)
                        }
                    },

                    async findFirst({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)

                        

                        try {
                            if (Block.debug) {
                                console.log('🔄 findFirst', {
                                    operation,
                                    args,
                                    model,
                                    query,
                                })
                            }
                            let result = await query(args)
                            Block.incrementOperationCount(blockId)
                            Block.updateLastSeenThrottled(blockId)
                            // If no result found in block, check main database
                            if (!result) {
                                if (Block.debug) {
                                    console.log(`[Sharded] No result found in block for ${model}.findFirst, checking main database`)
                                }

                                const mainResult = await (mainClient as any)[model.toLowerCase()][operation](args)
                                if (mainResult) {
                                    if (Block.debug) {
                                        console.log(`[Sharded] Found result in main database for ${model}.findFirst, reloading block`)
                                    }
                                    // Reload block data since we found the record in main DB
                                    await Block.reloadBlockData(blockId)
                                    // Try the query again after reload
                                    result = await query(args)
                                }
                            }

                            return result
                        } finally {
                            Block.decrementOperationCount(blockId)
                        }
                    },

                    async findMany({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)

                        Block.incrementOperationCount(blockId)
                        Block.updateLastSeenThrottled(blockId)

                        try {
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
                        } finally {
                            Block.decrementOperationCount(blockId)
                        }
                    },

                    async findUnique({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)

                        Block.incrementOperationCount(blockId)
                        Block.updateLastSeenThrottled(blockId)

                        try {
                            if (Block.debug) {
                                console.log('🔄 findUnique', {
                                    operation,
                                    args,
                                    model,
                                    query,
                                })
                            }
                            let result = await query(args)

                            // If no result found in block, check main database
                            if (!result) {
                                if (Block.debug) {
                                    console.log(`[Sharded] No result found in block for ${model}.findUnique, checking main database`)
                                }

                                const mainResult = await (mainClient as any)[model.toLowerCase()][operation](args)
                                if (mainResult) {
                                    if (Block.debug) {
                                        console.log(`[Sharded] Found result in main database for ${model}.findUnique, reloading block`)
                                    }
                                    // Reload block data since we found the record in main DB
                                    await Block.reloadBlockData(blockId)
                                    // Try the query again after reload
                                    result = await query(args)
                                }
                            }

                            return result
                        } finally {
                            Block.decrementOperationCount(blockId)
                        }
                    },

                    async findUniqueOrThrow({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)

                        

                        try {
                            if (Block.debug) {
                                console.log('🔄 findUniqueOrThrow', {
                                    operation,
                                    args,
                                    model,
                                    query,
                                })
                            }

                            try {
                                const result = await query(args)
                                Block.incrementOperationCount(blockId)
                                Block.updateLastSeenThrottled(blockId)
                                return result
                            } catch (error) {
                                // If the error is "Record not found" type, check main database
                                if ((error as any)?.code === 'P2025' || (error as any)?.message?.includes('No') || (error as any)?.message?.includes('not found')) {
                                    if (Block.debug) {
                                        console.log(`[Sharded] Record not found in block for ${model}.findUniqueOrThrow, checking main database`)
                                    }

                                    try {
                                        const mainResult = await (mainClient as any)[model.toLowerCase()][operation](args)
                                        if (mainResult) {
                                            if (Block.debug) {
                                                console.log(`[Sharded] Found result in main database for ${model}.findUniqueOrThrow, reloading block`)
                                            }
                                            // Reload block data since we found the record in main DB
                                            await Block.reloadBlockData(blockId)
                                            // Try the query again after reload
                                            const result = await query(args)
                                            return result
                                        }
                                    } catch (mainError) {
                                        // If main DB also doesn't have it, throw the original error
                                        throw error
                                    }
                                }
                                // Re-throw other types of errors
                                throw error
                            }
                        } finally {
                            Block.decrementOperationCount(blockId)
                        }
                    },

                    async findFirstOrThrow({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)

                        

                        try {
                            if (Block.debug) {
                                console.log('🔄 findFirstOrThrow', {
                                    operation,
                                    args,
                                    model,
                                    query,
                                })
                            }

                            try {
                                const result = await query(args)
                                Block.incrementOperationCount(blockId)
                                Block.updateLastSeenThrottled(blockId)
                                return result
                            } catch (error) {
                                // If the error is "Record not found" type, check main database
                                if ((error as any)?.code === 'P2025' || (error as any)?.message?.includes('No') || (error as any)?.message?.includes('not found')) {
                                    if (Block.debug) {
                                        console.log(`[Sharded] Record not found in block for ${model}.findFirstOrThrow, checking main database`)
                                    }

                                    try {
                                        const mainResult = await (mainClient as any)[model.toLowerCase()][operation](args)
                                        if (mainResult) {
                                            if (Block.debug) {
                                                console.log(`[Sharded] Found result in main database for ${model}.findFirstOrThrow, reloading block`)
                                            }
                                            // Reload block data since we found the record in main DB
                                            await Block.reloadBlockData(blockId)
                                            // Try the query again after reload
                                            const result = await query(args)
                                            return result
                                        }
                                    } catch (mainError) {
                                        // If main DB also doesn't have it, throw the original error
                                        throw error
                                    }
                                }
                                // Re-throw other types of errors
                                throw error
                            }
                        } finally {
                            Block.decrementOperationCount(blockId)
                        }
                    },

                    // these need to be pulled directly from the main client, Ideally you don't want to do this
                    async count({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)

                        Block.incrementOperationCount(blockId)
                        Block.updateLastSeenThrottled(blockId)

                        try {
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
                        } finally {
                            Block.decrementOperationCount(blockId)
                        }
                    },

                    async aggregate({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)

                        Block.incrementOperationCount(blockId)
                        Block.updateLastSeenThrottled(blockId)

                        try {
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
                        } finally {
                            Block.decrementOperationCount(blockId)
                        }
                    },

                    async groupBy({ operation, args, model, query }) {
                        // Wait for any locks (master creation/reload) to complete before executing
                        await Block.waitForBlockReady(blockId)

                        Block.incrementOperationCount(blockId)
                        Block.updateLastSeenThrottled(blockId)

                        try {
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
                        } finally {
                            Block.decrementOperationCount(blockId)
                        }
                    },
                },
            },
        })

        return newClient as typeof client & Prisma.DefaultPrismaClient
    }

    static async invalidate(blockId: string) {
        // Use the same block_lock as master creation/reload for mutual exclusion
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

        // Prevent double invalidation locally as well
        if (Block.invalidatingBlocks.has(blockId)) {
            if (Block.debug) {
                console.log('[Sharded] Block already being invalidated locally:', blockId)
            }
            // Release the Redis lock since we won't proceed
            await Block.redis?.del(blockLockKey)
            return
        }

        Block.invalidatingBlocks.add(blockId)

        try {
            if (Block.debug) {
                console.log('[Sharded] Starting invalidation of block:', blockId)
            }

            // Wait for active operations to complete
            const maxWaitTime = 30000 // 30 seconds max wait
            const startTime = Date.now()
            while (Block.activeOperations.has(blockId)) {
                if (Date.now() - startTime > maxWaitTime) {
                    console.warn(`[Sharded] Timeout waiting for operations to complete for block ${blockId}, forcing invalidation`)
                    break
                }
                if (Block.debug) {
                    console.log(`[Sharded] Waiting for ${Block.activeOperations.get(blockId)} active operations to complete for block ${blockId}`)
                }
                await new Promise(resolve => setTimeout(resolve, 100))
            }

            // Wait for pending sync operations to complete
            if (Block.debug) {
                console.log(`[Sharded] Waiting for pending syncs before invalidating block ${blockId}`)
            }
            await Block.waitForPendingSyncs(blockId, maxWaitTime)

            // Clean up Redis operation queue
            const operationQueueKey = `block_operations:${blockId}`
            await Block.redis?.del(operationQueueKey)

            // Get references to resources before removing from maps
            const blockClient = Block.blockClients.get(blockId)
            const blockClientWithHooks = Block.blockClientsWithHooks.get(blockId)
            const mainClient = Block.mainClients.get(blockId)
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

            // Always release the Redis block lock
            const blockLockKey = `block_lock:${blockId}`
            await Block.redis?.del(blockLockKey)
            if (Block.debug) {
                console.log(`[Sharded] Released block lock after invalidation: ${blockId}`)
            }
        }
    }

    /**
     * invalidation worker process
     *  - Will watch every block created on this node/machine
     *  - If block hasn't been accessed or written to longer than the ttl, configured on the master node
     *  - It will run this check every 10 seconds (override with interval) on all blocks
     * 
     * @param options
     * Watcher options:
     *  - ttl: in seconds, The default ttl for all blocks, if not set on the master node
     *  - interval: in milliseconds, The interval to check all blocks
     *  - connection: The connection options for the queue
     *  */

    static async watch(options: WatchOptions) {
        const queue = new Queue('invalidation', {
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

        queue.on("error", (error) => {
            console.error(`[sharded][watch] Unhandled error block invalidation queue: ${queue.name}`, error);
        });

        await queue.waitUntilReady()

        await queue.upsertJobScheduler("check-invalidation-every", {
            every: options.interval ?? 10000,
        }, {
            name: "check-invalidation-every",
            data: {
                ttl: options.ttl ?? 60,
            },
        })

        new Worker(queue.name, async (job: Job<{ ttl: number }>) => {
            const lastSeen = await Block.redis?.hgetall("last_seen")
            for (const blockId in lastSeen) {
                const lastSeenTime = parseInt(lastSeen[blockId])
                const blockTtl = parseInt((await Block.redis?.hget("block_ttl", blockId)) ?? `${job.data.ttl}`) * 1000
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
    }
}
