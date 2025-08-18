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

    static async create<T>(config: BlockConfig<T>) {
        const node = config.node ?? 'worker'
        if (Block.locks.has(config.blockId)) {
            if (Block.debug) {
                console.log('[Sharded] Has lock', config.blockId)
            }
            await Block.locks.get(config.blockId)
        }
        Block.debug = config.debug ?? false
        if (
            !Block.blockClientsWithHooks.has(config.blockId) ||
            !Block.blockClients.has(config.blockId) ||
            !Block.mainClients.has(config.blockId)
        ) {
            if (Block.debug) {
                console.log('[Sharded] Locking creation of block:', config.blockId)
            }
            Block.locks.set(config.blockId, new Promise(resolve => {
                const interval = setInterval(() => {
                    if (Block.debug) {
                        console.log('[Sharded] Checking lock', config.blockId)
                    }
                    if (Block.blockClientsWithHooks.has(config.blockId) &&
                        Block.blockClients.has(config.blockId) &&
                        Block.mainClients.has(config.blockId)) {
                        Block.locks.delete(config.blockId)
                        if (Block.debug) {
                            console.log('[Sharded] Unlocking creation of block:', config.blockId)
                        }
                        resolve()
                        clearInterval(interval)
                    }
                }, 500)
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

            // Enable WAL mode and set pragmas for better performance
            try {
                await blockClient.$queryRaw`PRAGMA journal_mode=WAL`
                await blockClient.$queryRaw`PRAGMA synchronous=NORMAL`
                await blockClient.$queryRaw`PRAGMA busy_timeout=5000`
                await blockClient.$queryRaw`PRAGMA cache_size=-2000` // Use 2MB of memory for cache
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
                await config.loader(blockClient as T, config.client as T)
            }

            if (Block.debug) {
                console.log('[Sharded] Created block:', config.blockId)
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
        return Block.blockClientsWithHooks.get(config.blockId)! as T
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

        return Block.blockQueues.get(blockId)!
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

            const mainClient = await Block.mainClients.get(job.queueName)
            if (!mainClient) {
                throw new Error(`Client for block ${job.queueName} not found`)
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
                    throw new Error(
                        `Duplicate record detected in ${model}. This might indicate a bug in your application logic.`,
                    )
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
            throw new Error(`Queue for block ${blockId} not found`)
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
                        Block.redis?.hset("last_seen", blockId, Date.now())
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
                        Block.redis?.hset("last_seen", blockId, Date.now())
                        await Block.queue_operation(blockId, {
                            operation,
                            model,
                            args,
                        })
                        return result
                    },

                    async update({ operation, args, model, query }) {
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
                        Block.redis?.hset("last_seen", blockId, Date.now())
                        return result
                    },

                    async updateMany({ operation, args, model, query }) {
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
                        Block.redis?.hset("last_seen", blockId, Date.now())
                        return result
                    },

                    async upsert({ operation, args, model, query }) {
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
                        Block.redis?.hset("last_seen", blockId, Date.now())
                        return result
                    },

                    async delete({ operation, args, model, query }) {
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
                        Block.redis?.hset("last_seen", blockId, Date.now())
                        // Queue the operation for main DB sync
                        await Block.queue_operation(blockId, {
                            operation,
                            model,
                            args,
                        })
                        return result
                    },

                    async deleteMany({ operation, args, model, query }) {
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
                        Block.redis?.hset("last_seen", blockId, Date.now())
                        // Queue the operation for main DB sync
                        await Block.queue_operation(blockId, {
                            operation,
                            model,
                            args,
                        })
                        return result
                    },

                    async findFirst({ operation, args, model, query }) {
                        if (Block.debug) {
                            console.log('🔄 findFirst', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }
                        const result = await query(args)
                        Block.redis?.hset("last_seen", blockId, Date.now())
                        return result
                    },

                    async findMany({ operation, args, model, query }) {
                        if (Block.debug) {
                            console.log('🔄 findMany', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }
                        const result = await query(args)
                        Block.redis?.hset("last_seen", blockId, Date.now())
                        return result
                    },

                    async findUnique({ operation, args, model, query }) {
                        if (Block.debug) {
                            console.log('🔄 findUnique', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }
                        const result = await query(args)
                        Block.redis?.hset("last_seen", blockId, Date.now())
                        return result
                    },

                    async findUniqueOrThrow({ operation, args, model, query }) {
                        if (Block.debug) {
                            console.log('🔄 findUniqueOrThrow', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }
                        const result = await query(args)
                        Block.redis?.hset("last_seen", blockId, Date.now())
                        return result
                    },

                    async findFirstOrThrow({ operation, args, model, query }) {
                        if (Block.debug) {
                            console.log('🔄 findFirstOrThrow', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }
                        const result = await query(args)
                        Block.redis?.hset("last_seen", blockId, Date.now())
                        return result
                    },

                    // these need to be pulled directly from the main client, Ideally you don't want to do this
                    async count({ operation, args, model, query }) {
                        if (Block.debug) {
                            console.log('🔄 count', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }
                        const result = await query(args)
                        Block.redis?.hset("last_seen", blockId, Date.now())
                        return result
                    },

                    async aggregate({ operation, args, model, query }) {
                        if (Block.debug) {
                            console.log('🔄 aggregate', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }
                        const result = await query(args)
                        Block.redis?.hset("last_seen", blockId, Date.now())
                        return result
                    },

                    async groupBy({ operation, args, model, query }) {
                        if (Block.debug) {
                            console.log('🔄 groupBy', {
                                operation,
                                args,
                                model,
                                query,
                            })
                        }
                        const result = await query(args)
                        Block.redis?.hset("last_seen", blockId, Date.now())
                        return result
                    },
                },
            },
        })

        return newClient as typeof client & Prisma.DefaultPrismaClient
    }

    static async invalidate(blockId: string) {
        if (Block.debug) {
            console.log('[Sharded] Destroying block:', blockId)
        }
        Block.blockClients.delete(blockId)
        Block.blockClientsWithHooks.delete(blockId)
        Block.mainClients.delete(blockId)
        Block.blockQueues.delete(blockId)

        // delete resources
        await Block.delete_block(blockId)

        // what do we do about the worker?
        const worker = Block.blockWorkers.get(blockId)
        if (worker) {
            if (Block.debug) {
                console.log('[Sharded] Closing worker:', blockId)
            }
            worker.close()
            Block.blockWorkers.delete(blockId)
        }
        return
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
                    await Block.invalidate(blockId)
                    Block.redis?.hdel("last_seen", blockId)
                    Block.redis?.hdel("block_ttl", blockId)
                    console.log('[Sharded] Invalidate block:', blockId)
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


