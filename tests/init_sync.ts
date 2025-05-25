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
    try {
        console.log('📦 Creating new block client...')
        const loader = async (
            blockClient: PrismaClient,
            mainClient: PrismaClient,
        ) => {
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

        console.log('\n👤 Creating test users...')
        const user_1 = await client.user.create({
            data: {
                email: 'test@example.com',
                name: 'Test User',
            },
        })
        console.log('✅ Created User 1:', user_1.name)
        assert(user_1.email === 'test@example.com', 'User 1 email should match')

        const user_2 = await client.user.create({
            data: {
                email: 'test2@example.com',
                name: 'Test User 2',
            },
        })
        console.log('✅ Created User 2:', user_2.name)
        assert(
            user_2.email === 'test2@example.com',
            'User 2 email should match',
        )

        console.log('\n👥 Creating multiple users in batch...')
        await client.user.createMany({
            data: [
                { email: 'test3@example.com', name: 'Test User 3' },
                { email: 'test4@example.com', name: 'Test User 4' },
            ],
        })

        const users = await client.user.findMany()
        console.log('📊 All users in block:', users)
        assert(
            users.length === 4,
            `Should have 4 users in total got ${users.length}`,
        )

        await new Promise((resolve) => setTimeout(resolve, 10000))
        console.log('\n⏳ Waiting 10 seconds for sync...')

        const main_users_after_creation = await main_client.user.findMany()
        console.log(
            '📊 Users in main DB after creation:',
            main_users_after_creation,
        )
        assert(
            main_users_after_creation.length === 4,
            'Main DB should have 4 users',
        )

        // Test updating and relationships
        console.log('\n✏️  Updating User 1...')
        await client.user.update({
            where: { id: user_1.id },
            data: {
                name: 'Test User 1 updated',
            },
        })

        const user_1_updated = await client.user.findUnique({
            where: { id: user_1.id },
        })
        console.log('✅ User 1 after update:', user_1_updated)
        assert(
            user_1_updated?.name === 'Test User 1 updated',
            'User 1 name should be updated',
        )

        await new Promise((resolve) => setTimeout(resolve, 10000))
        console.log('\n⏳ Waiting 10 seconds for sync...')

        const main_users_after_update = await main_client.user.findMany()
        console.log(
            '📊 Users in main DB after update:',
            main_users_after_update,
        )
        assert(
            main_users_after_update.length === 4,
            'Main DB should have 4 users',
        )

        const main_user_1_after_update = await main_client.user.findUnique({
            where: { id: user_1.id },
        })
        console.log(
            '📊 User 1 in main DB after update:',
            main_user_1_after_update,
        )
        assert(
            main_user_1_after_update?.name === 'Test User 1 updated',
            'User 1 name should be updated',
        )

        console.log('\n🛍️  Creating test order...')
        const order_1 = await client.order.create({
            data: {
                total: 99.99,
                userId: user_1.id,
            },
        })
        console.log('✅ Created order:', order_1)
        assert(order_1.total === 99.99, 'Order total should match')

        const orders = await client.order.findMany()
        console.log('📊 All orders in block:', orders)
        assert(orders.length === 1, 'Should have 1 order')

        console.log('\n🔍 Fetching users with their orders...')
        const user_1_with_orders = await client.user.findUnique({
            where: {
                id: user_1.id,
            },
            include: {
                orders: true,
            },
        })

        const user_2_with_orders = await client.user.findUnique({
            where: {
                id: user_2.id,
            },
            include: {
                orders: true,
            },
        })

        console.log('📊 User 1 with orders:', user_1_with_orders)
        console.log('📊 User 2 with orders:', user_2_with_orders)
        assert(
            user_1_with_orders?.orders.length === 1,
            'User 1 should have 1 order',
        )
        assert(
            user_2_with_orders?.orders.length === 0,
            'User 2 should have no orders',
        )

        await new Promise((resolve) => setTimeout(resolve, 10000))
        console.log('\n⏳ Waiting 10 seconds for sync...')

        const main_users_after_order_creation =
            await main_client.user.findMany()
        console.log(
            '📊 Users in main DB after order creation:',
            main_users_after_order_creation,
        )
        assert(
            main_users_after_order_creation.length === 4,
            'Main DB should have 4 users',
        )

        const main_orders_after_order_creation =
            await main_client.order.findMany()
        console.log(
            '📊 Orders in main DB after order creation:',
            main_orders_after_order_creation,
        )
        assert(
            main_orders_after_order_creation.length === 1,
            'Main DB should have 1 order',
        )

        // Test deletion
        console.log('\n🗑️  Deleting User 1...')
        await client.user.delete({
            where: { id: user_1.id },
        })
        console.log('✅ Deleted User 1')

        await client.user.delete({
            where: { id: user_2.id },
        })
        console.log('✅ Deleted User 2')

        await new Promise((resolve) => setTimeout(resolve, 10000))
        console.log('\n⏳ Waiting 10 seconds for sync...')

        const block_users_after_deletion = await client.user.findMany()
        console.log(
            '📊 Users in block DB after deletion:',
            block_users_after_deletion,
        )
        assert(
            block_users_after_deletion.length === 2,
            'Block DB should have 2 users remaining',
        )

        const block_orders_after_deletion = await client.order.findMany()
        console.log(
            '📊 Orders in block DB after deletion:',
            block_orders_after_deletion,
        )
        assert(
            block_orders_after_deletion.length === 0,
            'Block DB should have no orders after user deletion',
        )

        await new Promise((resolve) => setTimeout(resolve, 10000))
        console.log('\n⏳ Waiting 10 seconds for sync...')

        const main_users_after_deletion = await main_client.user.findMany()
        console.log(
            '📊 Users in main DB after deletion:',
            main_users_after_deletion,
        )
        assert(
            main_users_after_deletion.length === 2,
            'Main DB should have 2 users remaining',
        )

        const main_orders_after_deletion = await main_client.order.findMany()
        console.log(
            '📊 Orders in main DB after deletion:',
            main_orders_after_deletion,
        )
        assert(
            main_orders_after_deletion.length === 0,
            'Main DB should have no orders after user deletion',
        )

        await client.user.deleteMany()
        await client.order.deleteMany()

        await new Promise((resolve) => setTimeout(resolve, 10000))
        console.log('\n⏳ Waiting 10 seconds for sync...')

        const main_users_after_cleanup = await main_client.user.findMany()
        console.log(
            '📊 Users in main DB after cleanup:',
            main_users_after_cleanup,
        )
        assert(
            main_users_after_cleanup.length === 0,
            'Main DB should have no users after cleanup',
        )

        const main_orders_after_cleanup = await main_client.order.findMany()
        console.log(
            '📊 Orders in main DB after cleanup:',
            main_orders_after_cleanup,
        )
        assert(
            main_orders_after_cleanup.length === 0,
            'Main DB should have no orders after cleanup',
        )

        console.log('\n🗑️  Cleaning up block data...')
        rmSync(join(process.cwd(), 'prisma', 'blocks', 'data'), {
            recursive: true,
            force: true,
        })
        console.log('✅ Block data cleaned up')
    } catch (error) {
        console.error('❌ Block sync test failed:', error)
        process.exit(1)
    }

    // delete the block folder
    rmSync(join(process.cwd(), 'prisma', 'blocks'), { recursive: true, force: true })

    console.log('\n✨ All tests completed successfully!')

    await new Promise((resolve) => setTimeout(resolve, 10000))
    console.log('\n⏳ Waiting 10 seconds for sync...')
}

// Run tests
runTests()
    .catch((error) => {
        console.error('❌ Test runner failed:', error)
        process.exit(1)
    })
    .then(() => {
        process.exit(0)
    })
