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

    static async create<T>(config: BlockConfig<T>) {
        const node = config.node ?? 'worker'
        if (Block.locks.has(config.blockId)) {
            if (Block.debug) {
                console.log('[Sharded] Has lock', config.blockId)
            }
            await Block.locks.get(config.blockId)
        }
        Block.debug = config.debug ?? false
        let lockResolve: (() => void) | undefined
        if (
            !Block.blockClientsWithHooks.has(config.blockId) ||
            !Block.blockClients.has(config.blockId) ||
            !Block.mainClients.has(config.blockId)
        ) {
            if (Block.debug) {
                console.log('[Sharded] Locking creation of block:', config.blockId)
            }
            Block.locks.set(config.blockId, new Promise(resolve => {
                lockResolve = resolve
            }))

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

            // Resolve the lock after everything is complete (including loader)
            Block.locks.delete(config.blockId)
            if (lockResolve) {
                if (Block.debug) {
                    console.log('[Sharded] Unlocking creation of block:', config.blockId)
                }
                lockResolve()
            }
        }

        if (!Block.redis) {
            Block.redis = new Redis(config.connection ?? {
                host: process.env.REDIS_HOST ?? 'localhost',
                port: process.env.REDIS_PORT
                    ? parseInt(process.env.REDIS_PORT)
                    : 6379,
                password: process.env.REDIS_PASSWORD,
            })
        }


        if (config.node === 'master' && config.ttl) {
            Block.redis.hset("block_ttl", config.blockId, config.ttl)
        }

        // Tracking block for invalidation
        Block.redis.hset("last_seen", config.blockId, Date.now())

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
        const queue = Block.blockQueues.get(blockId)
        if (!queue) {
            if (Block.debug) {
                console.log(`[Sharded] No queue found for block ${blockId}, assuming no pending syncs`)
            }
            return
        }

        const startTime = Date.now()

        while (Date.now() - startTime < timeoutMs) {
            const waiting = await queue.getWaiting()
            const active = await queue.getActive()
            const delayed = await queue.getDelayed()

            const totalPending = waiting.length + active.length + delayed.length

            if (totalPending === 0) {
                if (Block.debug) {
                    console.log(`[Sharded] All syncs completed for block ${blockId}`)
                }
                return
            }

            if (Block.debug) {
                console.log(`[Sharded] Waiting for ${totalPending} pending syncs for block ${blockId}`)
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
                Block.redis?.hset("last_seen", blockId, Date.now())
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
     * Check if a block can be safely invalidated (no active operations)
     */
    static canInvalidate(blockId: string): boolean {
        return !Block.activeOperations.has(blockId) && !Block.invalidatingBlocks.has(blockId)
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
        if (!Block.blockQueues.has(blockId)) {
            const queue = new Queue(blockId, {
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

            queue.on("error", (error) => {
                console.error(`[sharded][blocks] Unhandled error in block_queue ${queue.name}`, error);
            });



            try {
                // Ensure connection is established
                await queue.waitUntilReady()
                Block.blockQueues.set(blockId, queue)
            } catch (err) {
                console.error('Error initializing queue:', err)
                throw err
            }
        }

        if (!Block.blockWorkers.has(blockId) && node === 'master') {
            // Only master can create sync workers as we want to avoid race conditions 
            // and multiple worker sync the operations on same block
            const worker = new Worker(blockId, Block.sync_operation, {
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
                console.error("[sharded] Unhandled error in block sync worker", error);
            });

            // Handling unexpected shutdowns
            process.on('SIGINT', () => {
                console.log(`[sharded] Received SIGINT, shutting down block sync worker`);
                worker.close();
            });
            process.on('SIGTERM', () => {
                console.log(`[sharded] Received SIGTERM, shutting down block sync worker`);
                worker.close();
            });

            try {
                // Ensure worker is ready
                await worker.waitUntilReady()
                Block.blockWorkers.set(blockId, worker)
            } catch (err) {
                console.error('Error initializing worker:', err)
                throw err
            }
        }

        const queue = Block.blockQueues.get(blockId)
        if (!queue) {
            throw new Error(`Failed to initialize queue for block ${blockId}`)
        }
        return queue
    }

    static async sync_operation(
        job: Job<{
            operation: string
            model: string
            args: any
            queued_at: number
        }>,
    ) {
        const { operation, model, args, queued_at } = job.data
        try {
            if (Block.debug) {
                console.log('sync_operation start', {
                    operation,
                    model,
                    args,
                    queued_at,
                    picked_after: Date.now() - queued_at,
                })
            }

            const mainClient = Block.mainClients.get(job.queueName)
            if (!mainClient) {
                if (Block.debug) {
                    console.log(`[Sharded] Main client for block ${job.queueName} not found - block may have been invalidated`)
                }
                return // Skip this operation as the block has been invalidated
            }

            try {
                await (mainClient as any)[model][operation](args)
            } catch (err: any) {
                // If it's a unique constraint violation, log it and throw
                if (err.code === 'P2002') {
                    const uniqueField = err.meta?.target?.[0]
                    console.error(
                        `[Sharded] Attempted to create duplicate record in ${model} with unique field ${uniqueField}:`,
                        {
                            blockId: job.queueName,
                            model,
                            data: args.data,
                            error: err,
                        },
                    )
                    // Re-throw the error to make it visible
                    throw err
                }
                throw err
            }
        } catch (err) {
            console.error('sync_operation', err)
            // Re-throw the error to ensure it's not silently handled
            throw err
        } finally {
            if (Block.debug) {
                console.log('sync_operation end', {
                    operation,
                    model,
                    args,
                    queued_at,
                })
            }
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
        const queue = Block.blockQueues.get(blockId)
        if (!queue) {
            if (Block.debug) {
                console.log(`[Sharded] Queue for block ${blockId} not found - block may have been invalidated`)
            }
            return // Skip this operation as the block has been invalidated
        }

        try {
            // Ensure connection is established
            await queue.waitUntilReady()

            // Add job with retry logic
            await queue.add(operation, {
                operation,
                model,
                args,
                queued_at: Date.now(),
            })
            if (Block.debug) {
                console.log('🔄 Queued operation:', { operation, model, args })
            }
        } catch (err) {
            console.error('Error queueing operation:', err)
            throw err
        }
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
                    async create({ operation, args, model, query }) {
                        Block.incrementOperationCount(blockId)
                        Block.redis?.hset("last_seen", blockId, Date.now())

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
                        Block.incrementOperationCount(blockId)
                        Block.redis?.hset("last_seen", blockId, Date.now())

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
                        Block.incrementOperationCount(blockId)
                        Block.redis?.hset("last_seen", blockId, Date.now())

                        try {
                            if (Block.debug) {
                                console.log('🔄 update', {
                                    operation,
                                    args,
                                    model,
                                    query,
                                })
                            }
                            await Block.queue_operation(blockId, {
                                operation,
                                model,
                                args,
                            })
                            const result = await query(args)
                            return result
                        } finally {
                            Block.decrementOperationCount(blockId)
                        }
                    },

                    async updateMany({ operation, args, model, query }) {
                        Block.incrementOperationCount(blockId)
                        Block.redis?.hset("last_seen", blockId, Date.now())

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
                        Block.incrementOperationCount(blockId)
                        Block.redis?.hset("last_seen", blockId, Date.now())

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
                        Block.incrementOperationCount(blockId)
                        Block.redis?.hset("last_seen", blockId, Date.now())

                        try {
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

                    async deleteMany({ operation, args, model, query }) {
                        Block.incrementOperationCount(blockId)
                        Block.redis?.hset("last_seen", blockId, Date.now())

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
                        Block.incrementOperationCount(blockId)
                        Block.redis?.hset("last_seen", blockId, Date.now())

                        try {
                            if (Block.debug) {
                                console.log('🔄 findFirst', {
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

                    async findMany({ operation, args, model, query }) {
                        Block.incrementOperationCount(blockId)
                        Block.redis?.hset("last_seen", blockId, Date.now())

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
                        Block.incrementOperationCount(blockId)
                        Block.redis?.hset("last_seen", blockId, Date.now())

                        try {
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
                        } finally {
                            Block.decrementOperationCount(blockId)
                        }
                    },

                    async findUniqueOrThrow({ operation, args, model, query }) {
                        Block.incrementOperationCount(blockId)
                        Block.redis?.hset("last_seen", blockId, Date.now())

                        try {
                            if (Block.debug) {
                                console.log('🔄 findUniqueOrThrow', {
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

                    async findFirstOrThrow({ operation, args, model, query }) {
                        Block.incrementOperationCount(blockId)
                        Block.redis?.hset("last_seen", blockId, Date.now())

                        try {
                            if (Block.debug) {
                                console.log('🔄 findFirstOrThrow', {
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

                    // these need to be pulled directly from the main client, Ideally you don't want to do this
                    async count({ operation, args, model, query }) {
                        Block.incrementOperationCount(blockId)
                        Block.redis?.hset("last_seen", blockId, Date.now())

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
                        Block.incrementOperationCount(blockId)
                        Block.redis?.hset("last_seen", blockId, Date.now())

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
                        Block.incrementOperationCount(blockId)
                        Block.redis?.hset("last_seen", blockId, Date.now())

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
        // Prevent double invalidation
        if (Block.invalidatingBlocks.has(blockId)) {
            if (Block.debug) {
                console.log('[Sharded] Block already being invalidated:', blockId)
            }
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
                    // Check if block has active operations before invalidating
                    if (Block.canInvalidate(blockId)) {
                        await Block.invalidate(blockId)
                        Block.redis?.hdel("last_seen", blockId)
                        Block.redis?.hdel("block_ttl", blockId)
                        console.log('[Sharded] Invalidated block:', blockId)
                    } else {
                        // Extend grace period for active blocks
                        const graceTime = Date.now() - (blockTtl * 0.8)
                        Block.redis?.hset("last_seen", blockId, graceTime)
                        if (Block.debug) {
                            console.log(`[Sharded] Extended grace period for active block: ${blockId}`)
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


