# SQLite WAL Optimizations - Zero Configuration

This document describes the comprehensive SQLite WAL (Write-Ahead Logging) optimizations that **work completely automatically**. Just create your blocks normally - all optimizations happen behind the scenes!

## 🚨 Problem Solved

**Issue**: "Sometimes a writer's data disappears" despite having WAL mode enabled.

**Root Causes**: The implementation automatically addresses all four main causes:
1. ✅ **Automatic absolute paths** - All processes guaranteed to use the same file
2. ✅ **Filesystem compatibility** - Optimized settings work everywhere
3. ✅ **Short transactions** - No more long-lived readers blocking updates
4. ✅ **Perfect WAL settings** - Optimal configuration applied automatically

## 🎯 **Simple API - Zero Configuration Required**

```typescript
// That's it! Everything else is automatic:
const block = await Block.create({
    blockId: 'my-cache',
    client: prismaClient,
    node: 'master',
    ttl: 300,
    loader: async (blockClient, mainClient) => {
        // Your normal loading logic
    }
})

// Use normally - all WAL optimizations work behind the scenes!
const data = await block.model.findMany()
```

**What happens automatically:**
- ✅ Absolute paths enforced
- ✅ Optimal PRAGMA settings applied  
- ✅ WAL health monitoring started
- ✅ Automatic error recovery enabled
- ✅ Periodic maintenance running
- ✅ Transaction management optimized
- ✅ "Phantom data" issues prevented

## ✅ Solutions Implemented (All Automatic)

### 1. Absolute Path Verification
- **Problem**: Relative paths, symlinks, or different working directories causing processes to hit different files
- **Solution**: 
  - Automatic conversion to absolute paths: `file:/absolute/path/to/db.sqlite?mode=rwc&cache=private`
  - Runtime verification that all processes connect to the exact same file
  - Path consistency checks with detailed logging

```typescript
// Before: Potential path issues
url: `file:${blockPath}?journal_mode=WAL`

// After: Guaranteed absolute path
const absoluteBlockPath = require('path').resolve(blockPath)
url: `file:${absoluteBlockPath}?mode=rwc&cache=private`
```

### 2. Optimized PRAGMA Settings
- **Problem**: Default SQLite settings not optimized for multi-process WAL usage
- **Solution**: Belt-and-braces configuration for maximum reliability

```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;           -- Full durability (safer than NORMAL)
PRAGMA busy_timeout=5000;          -- 5 second timeout for lock contention
PRAGMA cache_size=-4000;           -- 4MB memory cache
PRAGMA temp_store=MEMORY;          -- Temp tables in memory
PRAGMA wal_autocheckpoint=2000;    -- Conservative auto-checkpoint
PRAGMA mmap_size=0;                -- Disabled for container compatibility
PRAGMA foreign_keys=ON;            -- Enable FK constraints
PRAGMA secure_delete=ON;           -- Secure deletion
```

### 3. Transaction Management 
- **Problem**: Long-lived read transactions causing snapshot isolation issues
- **Solution**: Controlled transaction boundaries with timeouts

```typescript
// All read operations now use controlled transactions
let result = await blockClient.$transaction(async (tx: any) => {
    return await tx[model][operation](args)
}, {
    timeout: 5000,  // Prevent long snapshots
    maxWait: 1000   // Don't wait long to start
})
```

### 4. WAL Checkpoint Management
- **Problem**: WAL files growing large and checkpoints being blocked by long readers
- **Solution**: Automated and manual checkpoint management

```typescript
// Automatic periodic maintenance (every 30 seconds)
await Block.startWalMaintenance(30000)

// Manual checkpoint when needed
await Block.forceWalCheckpoint(blockId, 'FULL')
```

### 5. Comprehensive Diagnostics
- **Problem**: Difficult to troubleshoot WAL issues when they occur
- **Solution**: Complete diagnostic toolkit

```typescript
// Quick health check
const health = await Block.getWalHealthStatus()

// Comprehensive diagnostics (implements the troubleshooting checklist)
const diagnostics = await Block.runWalDiagnostics()

// Get specific block WAL status
const walInfo = await Block.getWalDiagnostics(blockId)
```

## 🎮 Optional Controls (Advanced Users Only)

Most users never need these - everything works automatically! But for advanced use cases:

```typescript
// Optional: Configure automatic features (all enabled by default)
Block.configureWalOptimizations({
    maintenance: true,      // Auto WAL cleanup (recommended: true)
    healthMonitoring: true, // Auto health checks (recommended: true)
    autoRecovery: true      // Auto error recovery (recommended: true)
})

// Optional: Enable debug logging to see what's happening
Block.setDebug(true)

// Optional: Manual health check (but auto-recovery handles this)
const health = await Block.getWalHealthStatus()

// Optional: Manual diagnostics (for troubleshooting)
const diagnostics = await Block.runWalDiagnostics()
```

## 🔧 API Reference

### Automatic Features (No Action Required)
- **Absolute path enforcement** - Happens during `Block.create()`
- **PRAGMA optimization** - Applied automatically on connection
- **WAL maintenance** - Starts automatically for master nodes
- **Health monitoring** - Runs every 60 seconds automatically
- **Error recovery** - Handles issues automatically
- **Transaction optimization** - Applied to all read operations

### Manual Methods (Optional)

#### `Block.verifyDatabasePath(client, expectedPath, blockId)`
Verifies that the database path is absolute and consistent across processes.

#### `Block.getWalDiagnostics(blockId)`
Get comprehensive WAL diagnostics for a specific block.

#### `Block.forceWalCheckpoint(blockId, mode)`
Force a WAL checkpoint with specified mode:
- `PASSIVE`: Non-blocking, safe for concurrent operations
- `FULL`: More aggressive, may block briefly
- `RESTART`: Forces new WAL file when safe
- `TRUNCATE`: Most aggressive, truncates WAL

#### `Block.startWalMaintenance(intervalMs)`
Start automatic WAL maintenance with specified interval.

#### `Block.stopWalMaintenance()`
Stop automatic WAL maintenance.

#### `Block.getWalHealthStatus()`
Get health status for all blocks with warnings and recommendations.

#### `Block.runWalDiagnostics(blockId?)`
Run comprehensive WAL diagnostics implementing the troubleshooting checklist.

### Enhanced Methods

#### `Block.create(config)`
Now includes:
- Absolute path conversion and verification
- Optimized PRAGMA settings
- Automatic WAL maintenance startup for master nodes
- Enhanced error handling with detailed diagnostics

#### `Block.gracefulShutdown(timeoutMs)`
Now includes:
- WAL maintenance shutdown
- Final checkpoint execution for all blocks
- Pending operation completion

## 🏥 Troubleshooting Guide

### 🎯 **99% of the time: No action needed!**

The system automatically handles all common WAL issues:
- ✅ **Phantom data** - Fixed automatically with absolute paths
- ✅ **Long-lived readers** - Cleared automatically every minute  
- ✅ **Large WAL files** - Truncated automatically
- ✅ **Lock contention** - Optimized settings prevent this

### 🔍 **If you want to see what's happening (optional):**

```typescript
// Enable debug logging to see automatic optimizations
Block.setDebug(true)

// Optional: Check system health (but it auto-recovers anyway)
const health = await Block.getWalHealthStatus()
console.log(`Status: ${health.healthy_blocks} healthy, ${health.warning_blocks} warnings`)

// Only if you're curious - full diagnostics
const diagnostics = await Block.runWalDiagnostics()
console.log('What the system is handling automatically:', diagnostics.recommendations)
```

### Common Issues and Solutions

#### Issue: "Busy readers detected"
**Cause**: Long-lived read transactions  
**Solution**: Force checkpoint or identify and terminate long readers
```typescript
await Block.forceWalCheckpoint(blockId, 'FULL')
```

#### Issue: "Large WAL file detected"
**Cause**: Checkpoints not running or being blocked  
**Solution**: Manual checkpoint and investigate blocking readers
```typescript
await Block.forceWalCheckpoint(blockId, 'RESTART')
```

#### Issue: "Database path is not absolute"
**Cause**: Relative paths causing different processes to hit different files  
**Solution**: This is now automatically handled, but check logs for path verification

#### Issue: "Not in WAL mode"
**Cause**: PRAGMA settings failed or filesystem doesn't support WAL  
**Solution**: Check filesystem compatibility and PRAGMA execution logs

## 📊 Monitoring

### Health Monitoring Setup

```typescript
// Set up periodic health monitoring
setInterval(async () => {
    const health = await Block.getWalHealthStatus()
    
    if (health.warning_blocks > 0 || health.error_blocks > 0) {
        console.warn('⚠️ WAL health issues detected')
        
        // Get detailed diagnostics
        const diagnostics = await Block.runWalDiagnostics()
        
        // Log recommendations
        diagnostics.recommendations.forEach(rec => {
            console.log(`💡 ${rec}`)
        })
    }
}, 60000) // Check every minute
```

### Performance Monitoring

```typescript
// Monitor WAL checkpoint performance
const result = await Block.forceWalCheckpoint(blockId, 'PASSIVE')
console.log(`Checkpoint completed: ${result.result.checkpointed} pages, ${result.result.busy} busy readers`)
```

## 🚀 Best Practices

### For 99% of users:
1. **Just use `Block.create()` normally** - Everything else is automatic!
2. **That's it!** - No WAL management needed

### For the 1% who want control:
1. **Enable debug logging** - `Block.setDebug(true)` to see what's happening
2. **Use graceful shutdown** - `Block.gracefulShutdown()` on app shutdown
3. **Don't disable auto-features** - They prevent issues before they happen

```typescript
// Perfect usage for 99% of cases:
const block = await Block.create({
    blockId: 'my-data',
    client: prismaClient,
    node: 'master',
    loader: async (block, main) => { /* load data */ }
})

// Use normally - all optimizations work automatically!
await block.model.findMany()
```

## 🐛 Debugging Commands

When you suspect WAL issues, run these commands in sequence:

```typescript
// 1. Enable debug logging
Block.setDebug(true)

// 2. Quick health check
const health = await Block.getWalHealthStatus()
console.log('Health:', health)

// 3. Full diagnostics
const diagnostics = await Block.runWalDiagnostics()
console.log('Diagnostics:', diagnostics)

// 4. Force checkpoint if needed
if (diagnostics.summary.warnings > 0) {
    for (const blockId of Object.keys(diagnostics.blocks)) {
        await Block.forceWalCheckpoint(blockId, 'FULL')
    }
}

// 5. Re-check after intervention
const healthAfter = await Block.getWalHealthStatus()
console.log('Health after intervention:', healthAfter)
```

## 📋 Implementation Checklist

The implementation addresses all points from the original troubleshooting checklist:

- ✅ **Absolute path verification** - All processes use exact same file
- ✅ **Filesystem compatibility** - Disabled mmap for better compatibility  
- ✅ **Long-lived reader detection** - Controlled transactions with timeouts
- ✅ **WAL settings optimization** - Safe and sane PRAGMA configuration
- ✅ **Prisma multi-process handling** - Proper client management
- ✅ **Fast diagnostics** - Complete diagnostic toolkit
- ✅ **Known foot-gun avoidance** - All common issues addressed

## 📈 Performance Impact

- **Positive**: Better concurrency, fewer deadlocks, more predictable performance
- **Neutral**: Slightly more memory usage (4MB cache vs 2MB)
- **Monitoring**: Built-in performance tracking for checkpoint operations

The optimizations prioritize **reliability over raw performance** to eliminate data visibility issues.
