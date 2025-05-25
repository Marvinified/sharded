import { generate } from '../cli/generate';
import { Block } from '../runtime/Block';
import { PrismaClient } from '@prisma/client';
import { join } from 'path';
import { rmSync } from 'fs';
import assert from 'assert';

async function runTests() {
  console.log('\n🧪 Starting buffered sync tests...');
  console.log('🗑️  Cleaning up previous test data...');
  rmSync(join(process.cwd(), 'prisma', 'blocks'), { recursive: true, force: true });

  try {
    const result = await generate({
      schema: './prisma/schema.prisma',
      models: ['User', 'Order'],
    });

    const main_client = new PrismaClient();
    const block_client = await Block.create({
      blockId: 'test',
      client: main_client,
      loader: async (blockClient: PrismaClient, mainClient: PrismaClient) => {
        // load the user from the main client
      },
    });

    const user_1 = await main_client.user.create({
      data: {
        email: 'test@example.com',
        name: 'Test User',
      },
    });

    console.time('fresh fetch');
    const user_1_from_block = await block_client.user.findUnique({
      where: {
        id: user_1.id,
      },
    });
    console.timeEnd('fresh fetch');
    console.log('user_1_from_block', user_1_from_block);

    console.time('cached fetch');
    const user_1_from_block_cached = await block_client.user.findUnique({
      where: {
        id: user_1.id,
      },
    });
    console.timeEnd('cached fetch');

    console.log('user_1_from_block_cached', user_1_from_block_cached);
    assert(user_1_from_block, 'User 1 should be in the block client');

    // create a new user
    const user_2 = await main_client.user.create({
      data: {
        email: 'test2@example.com',
        name: 'Test User 2',
      },
    });
    console.log('user_2 created in main client', user_2);


    console.time('count users from main client');
    const count_users_from_main = await main_client.user.count();
    console.timeEnd('count users from main client');
    console.log('count_users_from_main', count_users_from_main);

    console.time('fetch users from block');
    const users_from_block = await block_client.user.findMany();
    console.timeEnd('fetch users from block');
    console.log('users_from_block', users_from_block);


    // fetch user 2 from block
    console.time('fetch user 2 from block');
    const user_2_from_block = await block_client.user.findUnique({
      where: {
        id: user_2.id,
      },
    });
    console.timeEnd('fetch user 2 from block');


    // delete user_1 from main client
    await main_client.user.deleteMany();

  } catch (error) {
    console.error('❌ Buffered sync test failed:', error);
    process.exit(1);
  }
}

// Run tests
runTests().catch((error) => {
  console.error('❌ Test runner failed:', error);
  process.exit(1);
}).then(() => {
  console.log('✨ All tests completed successfully!');
  process.exit(0);
}); 