import { generate } from '../cli/generate'
import { Block } from '../runtime/Block'
import { PrismaClient } from '@prisma/client'
import { join } from 'path'
import { rmSync } from 'fs'
import assert from 'assert'

async function runTests() {
    console.log('\n🧪 Starting buffered sync tests...')
    console.log('🗑️  Cleaning up previous test data...')
    rmSync(join(process.cwd(), 'prisma', 'blocks'), { recursive: true, force: true })

    const result = await generate({
        schema: './prisma/schema.prisma',
        models: ['User', 'Order'],
    })

    const main_client = new PrismaClient()
    const block_client_1 = await Block.create({
        blockId: 'test_buffer',
        client: main_client,
        debug: true,
        loader: async (blockClient: PrismaClient, mainClient: PrismaClient) => {
            // load the user from the main client
        },
    })

    const user_1 = await block_client_1.user.create({
        data: {
            email: 'test@example.com',
            name: 'Test User',
        },
    })

    try {
        await new Promise((resolve) => setTimeout(resolve, 10000))

        const order_1 = await main_client.order.create({
            data: {
                userId: user_1.id,
                total: 100,
            },
        })

        console.log('order_1', order_1)

        const userWithOrder = await block_client_1.user.findUnique({
            where: {
                id: user_1.id,
            },
            include: {
                orders: true,
            },
        })

        console.log('userWithOrder', userWithOrder)

        await block_client_1.user.deleteMany()
        await block_client_1.order.deleteMany()

        //   // delete user_1 from main client
        //   await main_client.user.deleteMany();

        await Block.invalidate('test_buffer')
        await Block.invalidate('test_buffer')

        await new Promise((resolve) => setTimeout(resolve, 10000))
    } catch (error) {
        await main_client.user.deleteMany()
        await main_client.order.deleteMany()
        await block_client_1.user.deleteMany()
        await block_client_1.order.deleteMany()
        console.error('❌ Buffered sync test failed:', error)
        process.exit(1)
    }
}

// Run tests
runTests()
    .catch((error) => {
        console.error('❌ Test runner failed:', error)
        process.exit(1)
    })
    .then(() => {
        console.log('✨ All tests completed successfully!')
        process.exit(0)
    })
