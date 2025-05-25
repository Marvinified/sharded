import { generate } from '../cli/generate'
import { Block } from '../runtime/Block'
import { PrismaClient } from '@prisma/client'
import { join } from 'path'
import { rmSync } from 'fs'
import assert from 'assert'

/**
 * This test is used to test the block sync functionality
 * It creates a new block, creates some data, and then deletes the block
 * It then checks that the data was deleted
 */
async function runTests() {
    console.log('\n🧪 Starting block sync tests...')
    console.log('🗑️  Cleaning up previous test data...')
    rmSync(join(process.cwd(), 'prisma', 'blocks'), { recursive: true, force: true })

    console.log('\n📝 Testing schema generation...')
    let schemaPath: string
    let templatePath: string
    try {
        const result = await generate({
            schema: './prisma/schema.prisma',
            models: ['User', 'Order'],
        })

        schemaPath = result.schemaPath
        templatePath = result.templatePath

        console.log('✅ Schema generation successful')
        console.log('📄 Generated schema at:', schemaPath)
        console.log('💾 Template database at:', templatePath)

        // Assert schema files exist
        assert(schemaPath, 'Schema path should be defined')
        assert(templatePath, 'Template path should be defined')
    } catch (error) {
        console.error('❌ Schema generation failed:', error)
        process.exit(1)
    }

    console.log('\n🔄 Testing buffered sync...')
    console.log('📦 Creating new block client...')

    const loader = async (blockClient: PrismaClient, mainClient: PrismaClient) => {
        const users = await mainClient.user.findMany()
        for (const user of users) {
            await blockClient.user.create({
                data: user,
            })
        }

        const orders = await mainClient.order.findMany()
        for (const order of orders) {
            await blockClient.order.create({
                data: order,
            })
        }
    }

    const main_client = new PrismaClient()
    const client = await Block.create({
        blockId: 'test_buffer',
        client: main_client,
        loader,
    })
    await main_client.user.deleteMany()
    await main_client.order.deleteMany()

    try {
        console.log('\n👤 Creating test user...')
        const user = await client.user.create({
            data: {
                email: 'test@example.com',
                name: 'Test User',
            },
        })

        const order = await client.order.create({
            data: {
                total: 100,
                userId: user.id,
            },
        })

        console.log('\n⏳ Waiting 10 seconds for sync...')
        await new Promise((resolve) => setTimeout(resolve, 10000))
        console.log('🔄 Cleaning up block data...')
        rmSync(join(process.cwd(), 'prisma', 'blocks', 'data'), {
            recursive: true,
            force: true,
        })
        console.log('🔄 Destroying block...')
        await Block.invalidate('test_buffer')
        console.log('✅ Block destroyed')

        // clear the block data
        const new_client = await Block.create({
            blockId: 'test_buffer',
            client: main_client,
            loader,
        })

        // Wait for sync operations to complete
        console.log('\n⏳ Waiting for sync operations to complete...')
        await new Promise((resolve) => setTimeout(resolve, 5000))
        console.log('🔄 Creating new block client...')

        const order_data = await new_client.order.findUnique({
            where: {
                id: order.id,
            },
        })

        console.log({
            label: 'order',
            order_data,
        })

        // Assert that user_data is not null
        assert(order_data, 'Order data should not be null')
        assert(order_data.id === order.id, 'Order ID should match')
        assert(order_data.userId === user.id, 'Order user ID should match')

        rmSync(join(process.cwd(), 'prisma', 'blocks', 'data'), {
            recursive: true,
            force: true,
        })
        console.log('✅ Block data cleaned up')

        console.log('🔄 Waiting 10 seconds for sync...')
        await new Promise((resolve) => setTimeout(resolve, 10000))

        await Block.invalidate('test_buffer')
        console.log('✅ Main data cleaned up')
    } catch (error) {
        console.error('❌ Block sync test failed:', error)
        console.log('🔄 Waiting 10 seconds for sync...')
        await new Promise((resolve) => setTimeout(resolve, 10000))
        await Block.invalidate('test_buffer')
        await main_client.user.deleteMany()
        await main_client.order.deleteMany()
        console.log('✅ Main data cleaned up')
        process.exit(1)
    }

    // delete the block folder
    rmSync(join(process.cwd(), 'prisma', 'blocks'), { recursive: true, force: true })
    console.log('\n✨ All tests completed successfully!')
    await new Promise((resolve) => setTimeout(resolve, 10000))
    console.log('\n⏳ Waiting 10 seconds for sync...')
    process.exit(0)
}

// Run tests
runTests()
    .catch((error) => {
        console.error('❌ Test runner failed:', error)
        new Promise((resolve) => setTimeout(resolve, 10000)).then(() => {
            process.exit(1)
        })
    })
    .then(() => {
        process.exit(0)
    })
