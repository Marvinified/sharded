# Sharded - Make any database fast

[![npm version](https://badge.fury.io/js/sharded.svg)](https://badge.fury.io/js/sharded)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Sharded** is a SQLite-based write buffer and caching system for Prisma that provides high-performance data operations with automatic synchronization to your main database.

## ✨ **NEW: Automatic WAL Optimizations**

🎉 **Zero configuration required!** Sharded now automatically handles all SQLite WAL optimizations:

- **🔒 Prevents "phantom data" issues** - All processes use absolute paths automatically
- **⚡ Perfect SQLite settings** - Optimal PRAGMA configuration applied automatically  
- **🔄 Auto WAL maintenance** - Checkpointing and cleanup every 30 seconds
- **🏥 Health monitoring** - Auto-detection and recovery from WAL issues
- **⏱️ Smart transactions** - Short-lived reads prevent blocking
- **🛡️ Graceful fallbacks** - Automatic error recovery

Just use `Block.create()` normally - all optimizations work behind the scenes!

## 🚀 Performance Benefits

- **Dramatic Speed Improvement**: Reduce write & read times from >100ms to <10ms
- **10x faster reads** from SQLite cache vs. network database calls
- **Reduced database load** through write buffering
- **Improved user experience** with instant data access
- **Scalable architecture** supporting multiple worker nodes

## 🚀 Features

- **Write Buffering**: Buffer write operations in fast SQLite databases before syncing to your main database
- **Intelligent Caching**: Cache frequently accessed data for lightning-fast reads
- **Automatic Sync**: Background synchronization using Redis queues (BullMQ)
- **Prisma Integration**: Seamless integration with existing Prisma workflows
- **Multi-Node Support**: Master/worker architecture for distributed systems
- **Schema Generation**: CLI tools to generate optimized schemas for your blocks
- **WAL Mode**: Automatic WAL optimizations prevent "phantom data" issues ([details](./WAL-OPTIMIZATIONS.md))

## 🎯 When to Use Sharded

### ✅ Perfect Use Cases

- **Real-time applications** where sub-10ms response times are critical
- **High-frequency read/write operations** on specific data subsets
  - **Chat/messaging systems** with frequent message operations
  - **Gaming applications** requiring ultra-fast state updates
  - **Live collaboration tools** with real-time document editing
  - **Analytics dashboards** with frequently accessed metric

### ❌ When NOT to Use Sharded

- **Infrequent database operations** (< 10 operations per minute)
- **Simple CRUD applications** without performance bottlenecks
- **Applications with simple, linear data access patterns**
- **One-time data processing** or batch operations
- **Systems where network latency isn't a concern**
- **Applications with mostly write-once, read-rarely data**
- **Your database is fast enough** for your use case

## 📦 Installation

```bash
yarn add sharded
# or
npm install sharded
```

### Prerequisites

- Node.js 16+
- Prisma 6.7.0+
- Redis (for queue management)
- SQLite support

### Docker Deployment

When deploying with Docker, persist your blocks across redeployments by mounting the blocks data directory:

```dockerfile
# Dockerfile
FROM node:18-alpine

WORKDIR /app
COPY package*.json ./
RUN yarn install

COPY . .
RUN yarn build

# Create blocks directory
RUN mkdir -p /app/prisma/blocks/data

EXPOSE 3000
CMD ["yarn", "start"]
```

```yaml
# docker-compose.yml
version: '3.8'
services:
  app:
    build: .
    ports:
      - "3000:3000"
    volumes:
      # Persist blocks across container restarts/redeployments
      - blocks_data:/app/prisma/blocks/data
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/mydb
      - REDIS_URL=redis://redis:6379
    depends_on:
      - db
      - redis

  db:
    image: postgres:15
    environment:
      POSTGRES_DB: mydb
      POSTGRES_USER: user
      POSTGRES_PASSWORD: pass
    volumes:
      - postgres_data:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    volumes:
      - redis_data:/data

volumes:
  blocks_data:    # Persists your Sharded blocks
  postgres_data:  # Persists your main database
  redis_data:     # Persists Redis queue data
```

**Important**: Without persisting `/app/prisma/blocks/data`, your blocks will be recreated on every deployment, losing cached data and requiring full reloads.

## 🛠️ Quick Start

### 1. Generate Block Schema

First, generate a subset schema for the models you want to cache/buffer:

```bash
# Generate schema for specific models
npx sharded generate --schema=./prisma/schema.prisma --models=User,Order

# Or generate for all models
npx sharded generate --schema=./prisma/schema.prisma --all-models
```

This creates:

- `prisma/blocks/block.prisma` - Optimized schema for SQLite
- `prisma/blocks/template.sqlite` - Template database
- `prisma/blocks/generated/` - Generated Prisma client

### 2. Create a Block

```typescript
import { Block } from "sharded";
import { PrismaClient } from "@prisma/client";

const mainClient = new PrismaClient();

// Define how to load initial data into the block
const loader = async (blockClient: PrismaClient, mainClient: PrismaClient) => {
  // Load users from main database
  const users = await mainClient.user.findMany();
  for (const user of users) {
    await blockClient.user.create({ data: user });
  }

  // Load orders from main database
  const orders = await mainClient.order.findMany();
  for (const order of orders) {
    await blockClient.order.create({ data: order });
  }
};

// Create block client
const blockClient = await Block.create({
  blockId: "user-orders-cache",
  client: mainClient,
  loader,
  node: "master", // or 'worker'
  ttl: 3600, // Cache TTL in seconds (master only)
  connection: {
    host: "localhost",
    port: 6379,
    // password: 'your-redis-password'
  },
});
```

### 3. Use the Block Client

The block client works exactly like a regular Prisma client:

```typescript
// Create operations are buffered and synced asynchronously
const user = await blockClient.user.create({
  data: {
    email: "user@example.com",
    name: "John Doe",
  },
});

// Read operations use cached data when available
const users = await blockClient.user.findMany({
  include: {
    orders: true,
  },
});

// Updates are buffered and synced
await blockClient.user.update({
  where: { id: user.id },
  data: { name: "Jane Doe" },
});

// Deletes are buffered and synced
await blockClient.user.delete({
  where: { id: user.id },
});
```

## 🏗️ Architecture

### Master/Worker Nodes

- **Master Node**: Manages cache invalidation and TTL
- **Worker Node**: Processes write operations from the queue

```typescript
// Master node (handles cache invalidation)
const masterBlock = await Block.create({
  blockId: "my-cache",
  client: mainClient,
  loader,
  node: "master",
  ttl: 3600, // 1 hour cache
});

// Worker node (processes writes)
const workerBlock = await Block.create({
  blockId: "my-cache",
  client: mainClient,
  loader,
  node: "worker",
});
```

### Data Flow

1. **Writes**: Buffered in SQLite → Queued in Redis → Synced to main DB
2. **Reads**: Check SQLite cache → Fallback to main DB → Cache result
3. **Invalidation**: Automatic TTL-based or manual invalidation

## 📚 API Reference

### Block.create(config)

Creates a new block instance.

```typescript
interface BlockConfig<T> {
  blockId: string;                    // Unique identifier for the block
  client: T;                         // Main Prisma client
  loader: (blockClient: T, mainClient: T) => Promise<void>; // Data loader function
  debug?: boolean;                   // Enable debug logging
  prismaOptions?: Prisma.PrismaClientOptions; // Additional Prisma options
  connection?: {                     // Redis connection options
    host: string;
    port: number;
    password?: string;
  };
} & (
  { node: 'master'; ttl?: number; } |
  { node?: 'worker'; }
);
```

### Block.invalidate(blockId)

Manually invalidate a block cache:

```typescript
await Block.invalidate("user-orders-cache");
```

### Block.delete_block(blockId)

Delete a block and its associated files:

```typescript
await Block.delete_block("user-orders-cache");
```

### Block.watch(options)

Start watching for cache invalidation and orphaned queue recovery (master nodes):

```typescript
await Block.watch({
  ttl: 3600,
  interval: 60000, // Check every minute
  orphanedQueueCheckInterval: 30000, // Check for orphaned queues every 30 seconds
  mainClient: prismaClient, // Required for orphaned queue recovery
  connection: {
    host: "localhost",
    port: 6379,
  },
});
```

**Important**: The `mainClient` parameter is crucial for orphaned queue recovery. When a process restarts, the in-memory `Block.mainClients` map is empty, but Redis still contains queued operations. The provided `mainClient` enables recovery of these orphaned operations.

### Block.getOrphanedQueues()

Get information about blocks with queued operations but no active workers:

```typescript
const orphanedQueues = await Block.getOrphanedQueues();
console.log(orphanedQueues);
// Output: [{ blockId: "user-cache", queueLength: 5, hasWorker: false, hasQueue: true }]
```

### Block.checkOrphanedQueues(connection?, mainClient?)

Manually check for and restart workers for orphaned queues:

```typescript
// Basic usage (uses existing mainClients if available)
await Block.checkOrphanedQueues();

// With main client for orphaned recovery
await Block.checkOrphanedQueues(undefined, prismaClient);
```

### Block.createEphemeralRecoveryWorker(blockId, mainClient, connection?)

Manually create an ephemeral recovery worker for a specific block (automatically called by `checkOrphanedQueues`):

```typescript
const mainClient = Block.mainClients.get("user-cache");
await Block.createEphemeralRecoveryWorker("user-cache", mainClient);
// Creates actual BullMQ worker that reuses batch_sync_operation logic
// Processes all operations until queue is empty, then automatically closes itself
```

## ⚠️ Known Limitations

- **Cache Consistency**: If records are modified directly in the main database, the block cache won't be aware until invalidation

## 📋 TODO

- **Multi-Machine Sync**: Currently, blocks with the same ID across different machines don't sync with each other. Multi-process on the same machine works fine as they share the same SQLite file location, but cross-machine block synchronization needs to be implemented.

## 🧪 Testing

```bash
# Run tests
yarn test

# Run in development mode
yarn dev
```

## 📁 Project Structure

```
sharded/
├── cli/                 # Command-line interface
│   ├── generate.ts     # Schema generation
│   └── index.ts        # CLI entry point
├── runtime/            # Core runtime
│   └── Block.ts        # Main Block class
├── tests/              # Test files
├── prisma/             # Example schema
└── dist/               # Compiled output
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Commit your changes: `git commit -m 'Add amazing feature'`
4. Push to the branch: `git push origin feature/amazing-feature`
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Links

- [npm package](https://www.npmjs.com/package/sharded)
- [GitHub repository](https://github.com/your-username/sharded)
- [Issues](https://github.com/your-username/sharded/issues)

## 💡 Use Cases

- **High-traffic applications** requiring fast read access
- **Microservices** needing local data caching
- **Real-time applications** with frequent database operations
- **Analytics workloads** requiring fast aggregations
- **Multi-tenant applications** with isolated data blocks

## 🎯 Block Scoping Strategy

**Important**: Sharded is designed for **subsections** of your application that need fast read/writes, not entire databases. Loading your entire database into a block would be inefficient and defeat the purpose.

### ✅ Good Block Scoping Examples

#### 1. **Per-User Blocks** (User Dashboard)

```typescript
// Block for a specific user's data
const userBlock = await Block.create({
  blockId: `user-${userId}`,
  client: mainClient,
  loader: async (blockClient, mainClient) => {
    // Load only this user's data
    const user = await mainClient.user.findUnique({
      where: { id: userId },
      include: {
        profile: true,
        settings: true,
        recentActivity: { take: 50 },
      },
    });

    if (user) {
      await blockClient.user.create({ data: user });
    }
  },
});
```

#### 2. **Per-Chat Blocks** (Messaging App)

```typescript
// Block for a specific chat room
const chatBlock = await Block.create({
  blockId: `chat-${chatId}`,
  client: mainClient,
  loader: async (blockClient, mainClient) => {
    // Load chat and recent messages
    const chat = await mainClient.chat.findUnique({
      where: { id: chatId },
      include: {
        messages: {
          take: 100, // Last 100 messages
          orderBy: { createdAt: "desc" },
        },
        participants: true,
      },
    });

    if (chat) {
      await blockClient.chat.create({ data: chat });
    }
  },
});

// Fast message operations
await chatBlock.message.create({
  data: {
    content: "Hello!",
    chatId: chatId,
    userId: senderId,
  },
});
```

#### 3. **Per-Session Blocks** (E-commerce Cart)

```typescript
// Block for user's shopping session
const sessionBlock = await Block.create({
  blockId: `session-${sessionId}`,
  client: mainClient,
  loader: async (blockClient, mainClient) => {
    // Load cart, wishlist, and recently viewed
    const session = await mainClient.session.findUnique({
      where: { id: sessionId },
      include: {
        cart: { include: { items: true } },
        wishlist: { include: { items: true } },
        recentlyViewed: { take: 20 },
      },
    });

    if (session) {
      await blockClient.session.create({ data: session });
    }
  },
});
```

#### 4. **Per-Game Blocks** (Gaming Application)

```typescript
// Block for active game state
const gameBlock = await Block.create({
  blockId: `game-${gameId}`,
  client: mainClient,
  loader: async (blockClient, mainClient) => {
    // Load game state and player data
    const game = await mainClient.game.findUnique({
      where: { id: gameId },
      include: {
        players: true,
        gameState: true,
        moves: { take: 50 }, // Recent moves
      },
    });

    if (game) {
      await blockClient.game.create({ data: game });
    }
  },
});

// Ultra-fast game moves
await gameBlock.move.create({
  data: {
    gameId,
    playerId,
    action: "attack",
    coordinates: { x: 10, y: 15 },
  },
});
```

#### 5. **Per-Workspace Blocks** (Collaboration Tools)

```typescript
// Block for team workspace
const workspaceBlock = await Block.create({
  blockId: `workspace-${workspaceId}`,
  client: mainClient,
  loader: async (blockClient, mainClient) => {
    // Load workspace with active documents and team members
    const workspace = await mainClient.workspace.findUnique({
      where: { id: workspaceId },
      include: {
        documents: {
          where: { status: "active" },
          take: 50,
        },
        members: true,
        recentActivity: { take: 100 },
      },
    });

    if (workspace) {
      await blockClient.workspace.create({ data: workspace });
    }
  },
});
```

### ❌ Avoid These Patterns

```typescript
// ❌ DON'T: Load entire database
const badBlock = await Block.create({
  blockId: "entire-app",
  loader: async (blockClient, mainClient) => {
    // This will be slow and memory-intensive
    const allUsers = await mainClient.user.findMany(); // Could be millions
    const allOrders = await mainClient.order.findMany(); // Could be millions
    // ... loading everything
  },
});

// ❌ DON'T: Overly broad scoping
const tooBroadBlock = await Block.create({
  blockId: "all-users-data",
  loader: async (blockClient, mainClient) => {
    // Loading all users when you only need one
    const users = await mainClient.user.findMany({
      include: { orders: true, profile: true },
    });
  },
});
```

### 🎯 Scoping Best Practices

1. **Scope by User/Session**: Create blocks per user session or user context
2. **Scope by Feature**: Create blocks for specific features (chat, cart, game)
3. **Limit Data Size**: Only load what you need (recent messages, active items)
4. **Use TTL Wisely**: Set appropriate cache expiration based on data freshness needs
5. **Monitor Block Size**: Keep blocks under 100MB for optimal performance

### 🔄 Block Lifecycle Management

```typescript
// Create block when user starts session
const userBlock = await Block.create({
  blockId: `user-${userId}-${sessionId}`,
  // ... config
});

// Use throughout session for fast operations
await userBlock.user.update({ ... });
await userBlock.activity.create({ ... });

// Clean up when session ends
await Block.delete_block(`user-${userId}-${sessionId}`);
```

---

Made with ❤️ by the Sharded team
